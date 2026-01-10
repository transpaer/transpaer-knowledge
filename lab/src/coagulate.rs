// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::{BTreeMap, btree_map::Entry};

use serde::{Deserialize, Serialize};

use transpaer_collecting::errors::{MapIo, MapSerde};
use transpaer_models::gather;

use crate::{
    errors::CoagulationError,
    substrate::{DataSetId, Substrate, Substrates},
};

/// An ID unique within a context inside of a single substrate file.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct InnerId(String);

impl InnerId {
    #[must_use]
    pub fn new(id: String) -> Self {
        Self(id)
    }
}

impl std::fmt::Display for InnerId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Combines a data set ID and an inner ID, thus identifying an entry uniquely accross all substrate files.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct ExternalId {
    data_set_id: DataSetId,
    inner: String,
}

impl ExternalId {
    #[must_use]
    pub fn new(data_set_id: DataSetId, inner: InnerId) -> Self {
        Self { data_set_id, inner: inner.0 }
    }

    #[must_use]
    pub fn decompose(self) -> (DataSetId, InnerId) {
        (self.data_set_id, InnerId(self.inner))
    }

    #[must_use]
    pub fn to_error_not_found(&self, substrate: &Substrate, when: &str) -> CoagulationError {
        CoagulationError::UniqueIdNotFoundForInnerId {
            data_set_path: substrate.path.clone(),
            inner_id: self.inner.clone(),
            when: when.to_owned(),
        }
    }
}

pub trait UniqueId: Clone + Eq + Ord + std::hash::Hash {
    fn zero() -> Self;
    fn increment(&mut self);
}

/// ID maps for producers and products.
#[must_use]
pub struct Coagulate {
    /// Producer ID map.
    producer: BTreeMap<ExternalId, gather::OrganisationId>,

    /// Product ID map.
    product: BTreeMap<ExternalId, gather::ProductId>,
}

#[derive(Serialize, Deserialize)]
struct ExternalEntry {
    #[serde(rename = "s")]
    dataset: String,

    #[serde(rename = "i")]
    inner: String,
}

#[derive(Serialize, Deserialize)]
struct CoagulateData {
    /// Producer ID map.
    producer: BTreeMap<gather::OrganisationId, Vec<ExternalEntry>>,

    /// Product ID map.
    product: BTreeMap<gather::ProductId, Vec<ExternalEntry>>,
}

impl Coagulate {
    pub fn new(
        producer: BTreeMap<ExternalId, gather::OrganisationId>,
        product: BTreeMap<ExternalId, gather::ProductId>,
    ) -> Self {
        Self { producer, product }
    }

    pub fn get_unique_id_for_producer_external_id(
        &self,
        external_id: &ExternalId,
    ) -> Result<gather::OrganisationId, ExternalId> {
        if let Some(unique_id) = self.producer.get(external_id) {
            Ok(unique_id.clone())
        } else {
            Err(external_id.clone())
        }
    }

    pub fn get_unique_id_for_product_external_id(
        &self,
        external_id: &ExternalId,
    ) -> Result<gather::ProductId, ExternalId> {
        if let Some(unique_id) = self.product.get(external_id) {
            Ok(unique_id.clone())
        } else {
            Err(external_id.clone())
        }
    }

    pub fn save(
        self,
        path: &std::path::Path,
        substrates: &Substrates,
    ) -> Result<(), CoagulationError> {
        let mut producer = BTreeMap::<gather::OrganisationId, Vec<ExternalEntry>>::new();
        let mut product = BTreeMap::<gather::ProductId, Vec<ExternalEntry>>::new();

        for (external, unique) in self.producer {
            let entry = ExternalEntry {
                dataset: substrates
                    .get_name_for_id(external.data_set_id)
                    .ok_or(CoagulationError::SubstrateNameNotFoundForId {
                        id: external.data_set_id,
                    })?
                    .to_owned(),
                inner: external.inner,
            };
            match producer.entry(unique) {
                Entry::Vacant(e) => {
                    e.insert(vec![entry]);
                }
                Entry::Occupied(mut e) => {
                    e.get_mut().push(entry);
                }
            }
        }

        for (external, unique) in self.product {
            let entry = ExternalEntry {
                dataset: substrates
                    .get_name_for_id(external.data_set_id)
                    .ok_or(CoagulationError::SubstrateNameNotFoundForId {
                        id: external.data_set_id,
                    })?
                    .to_owned(),
                inner: external.inner,
            };
            match product.entry(unique) {
                Entry::Vacant(e) => {
                    e.insert(vec![entry]);
                }
                Entry::Occupied(mut e) => {
                    e.get_mut().push(entry);
                }
            }
        }

        let data = CoagulateData { producer, product };
        let serialized = serde_yaml::to_string(&data).map_serde()?;
        std::fs::write(path, serialized).map_with_path(path)?;
        Ok(())
    }

    pub fn read(path: &std::path::Path, substrates: &Substrates) -> Result<Self, CoagulationError> {
        log::info!("Reading the coagulate...");
        let contents = std::fs::read_to_string(path).map_with_path(path)?;
        let parsed: CoagulateData = serde_yaml::from_str(&contents).map_with_path(path)?;

        let mut producer = BTreeMap::<ExternalId, gather::OrganisationId>::new();
        let mut product = BTreeMap::<ExternalId, gather::ProductId>::new();

        for (unique, entries) in parsed.producer {
            for entry in entries {
                let external = ExternalId {
                    data_set_id: *substrates.get_id_for_name(&entry.dataset).ok_or(
                        CoagulationError::SubstrateIdNotFoundForName {
                            name: entry.dataset.clone(),
                        },
                    )?,
                    inner: entry.inner.clone(),
                };
                match producer.entry(external.clone()) {
                    Entry::Vacant(e) => {
                        e.insert(unique.clone());
                    }
                    Entry::Occupied(_) => {
                        return Err(CoagulationError::ExternalIdRepeated { id: external });
                    }
                }
            }
        }

        for (unique, entries) in parsed.product {
            for entry in entries {
                let external = ExternalId {
                    data_set_id: *substrates.get_id_for_name(&entry.dataset).ok_or(
                        CoagulationError::SubstrateIdNotFoundForName {
                            name: entry.dataset.clone(),
                        },
                    )?,
                    inner: entry.inner.clone(),
                };
                match product.entry(external.clone()) {
                    Entry::Vacant(e) => {
                        e.insert(unique.clone());
                    }
                    Entry::Occupied(_) => {
                        return Err(CoagulationError::ExternalIdRepeated { id: external });
                    }
                }
            }
        }

        log::info!("Reading the coagulate... done");
        Ok(Coagulate { producer, product })
    }
}
