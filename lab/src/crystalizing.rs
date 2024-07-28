use std::collections::{btree_map::Entry, BTreeMap, BTreeSet, HashSet};

use merge::Merge;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;

use sustainity_models::{collections, gather, ids, store};
use sustainity_schema as schema;

use crate::{config, errors, utils};

const MAX_CATEGORY_PRODUCT_NUM: usize = 300_000;

fn extract_regions(
    availability: Option<&schema::ProductAvailability>,
) -> Result<gather::Regions, isocountry::CountryCodeParseErr> {
    if let Some(availability) = availability {
        convert_regions(&availability.regions)
    } else {
        Ok(gather::Regions::Unknown)
    }
}

fn convert_regions(
    regions: &schema::Regions,
) -> Result<gather::Regions, isocountry::CountryCodeParseErr> {
    Ok(match regions {
        schema::Regions::Variant(variant) => match variant {
            schema::RegionVariant::All => gather::Regions::World,
            schema::RegionVariant::Unknown => gather::Regions::Unknown,
        },
        schema::Regions::List(list) => {
            let mut regions = Vec::new();
            for region in &list.0 {
                regions.push(isocountry::CountryCode::for_alpha3(region)?);
            }
            gather::Regions::List(regions)
        }
    })
}

// Indentifies a substrate file.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DataSetId(usize);

impl DataSetId {
    #[must_use]
    pub fn new(id: usize) -> Self {
        Self(id)
    }
}

#[derive(Debug)]
pub struct Substrate {
    id: DataSetId,
    path: std::path::PathBuf,
    source: gather::Source,
}

pub struct Substrates {
    list: Vec<Substrate>,
}

impl Substrates {
    pub fn prepare(
        directory: &std::path::Path,
    ) -> Result<(Self, CrystalizationReport), errors::ProcessingError> {
        let mut report = CrystalizationReport::default();
        let mut list = Vec::new();

        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(stem) = path.file_stem() {
                    if let Some(stem) = stem.to_str() {
                        list.push(Substrate {
                            id: DataSetId::new(list.len()),
                            path: path.clone(),
                            source: gather::Source::from_string(stem),
                        });
                    } else {
                        report.add_path_not_unicode(path.clone());
                    }
                } else {
                    report.add_no_file_stem(path.clone());
                }
            }
        }

        Ok((Self { list }, report))
    }

    #[must_use]
    pub fn list(&self) -> &[Substrate] {
        &self.list
    }

    #[must_use]
    pub fn get_path_for_id(&self, data_set_id: DataSetId) -> Option<&std::path::Path> {
        for substrate in &self.list {
            if substrate.id == data_set_id {
                return Some(&substrate.path);
            }
        }
        None
    }
}

/// Warnings specific to the crystalisation command.
#[must_use]
#[derive(Error, Debug)]
pub enum CrystalizationWarning {
    // TODO: add more detailed info about the data set and ID variant.
    #[error("Individual ID is not valid: {individual_id}, data_set: {data_set_id:?})")]
    InvalidIndividualId { data_set_id: DataSetId, individual_id: String },
}

// TODO: Rework as repotts per data source
#[must_use]
#[derive(Debug, Default)]
pub struct CrystalizationReport {
    no_stem: BTreeSet<std::path::PathBuf>,
    not_unicode: BTreeSet<std::path::PathBuf>,
    invalid_ids: BTreeMap<DataSetId, BTreeSet<String>>,
    empty_ids: BTreeMap<DataSetId, BTreeSet<InnerId>>,
    missing_inner_ids: BTreeMap<DataSetId, BTreeSet<InnerId>>,
}

impl CrystalizationReport {
    pub fn add(&mut self, warning: CrystalizationWarning) {
        match warning {
            CrystalizationWarning::InvalidIndividualId { data_set_id, individual_id } => {
                self.add_invalid_id(data_set_id, individual_id);
            }
        }
    }

    pub fn add_many(&mut self, warnings: Vec<CrystalizationWarning>) {
        for warning in warnings {
            self.add(warning);
        }
    }

    pub fn add_no_file_stem(&mut self, path: std::path::PathBuf) {
        self.no_stem.insert(path);
    }

    pub fn add_path_not_unicode(&mut self, path: std::path::PathBuf) {
        self.not_unicode.insert(path);
    }

    pub fn add_invalid_id(&mut self, data_set_id: DataSetId, id: String) {
        match self.invalid_ids.entry(data_set_id) {
            Entry::Occupied(mut e) => {
                e.get_mut().insert(id);
            }
            Entry::Vacant(e) => {
                let mut set = BTreeSet::new();
                set.insert(id);
                e.insert(set);
            }
        }
    }

    pub fn add_missing_external_id(&mut self, external_id: ExternalId) {
        let (data_set, inner) = external_id.decompose();
        self.add_missing_inner_id(data_set, inner);
    }

    pub fn add_missing_inner_id(&mut self, data_set_id: DataSetId, inner_id: InnerId) {
        match self.missing_inner_ids.entry(data_set_id) {
            Entry::Occupied(mut e) => {
                e.get_mut().insert(inner_id);
            }
            Entry::Vacant(e) => {
                let mut set = BTreeSet::new();
                set.insert(inner_id);
                e.insert(set);
            }
        }
    }

    pub fn merge(&mut self, other: Self) {
        self.no_stem.extend(other.no_stem);
        self.not_unicode.extend(other.not_unicode);
        utils::merge_btreemaps_with(&mut self.invalid_ids, other.invalid_ids, |a, b| {
            a.extend(b.iter().cloned());
        });
        utils::merge_btreemaps_with(&mut self.empty_ids, other.empty_ids, |a, b| {
            a.extend(b.iter().cloned());
        });
        utils::merge_btreemaps_with(
            &mut self.missing_inner_ids,
            other.missing_inner_ids,
            |a, b| {
                a.extend(b.iter().cloned());
            },
        );
    }

    pub fn report(&self, substrates: &Substrates) {
        const UNKNOWN: &str = "unknown";

        log::warn!("Crystalisation report:");

        if !self.no_stem.is_empty() {
            log::warn!(" no stem:");
            for path in &self.no_stem {
                log::warn!("  - {:?}", path);
            }
        }
        if !self.not_unicode.is_empty() {
            log::warn!(" not unicode:");
            for path in &self.not_unicode {
                log::warn!("  - {:?}", path);
            }
        }
        if !self.invalid_ids.is_empty() {
            log::warn!(" invalid IDs:");
            for (data_set_id, ids) in &self.invalid_ids {
                let path = substrates
                    .get_path_for_id(*data_set_id)
                    .map_or_else(|| UNKNOWN.to_string(), |path| format!("{path:?}"));
                log::warn!("  - {}: {}", path, ids.len());
            }
        }
        if !self.empty_ids.is_empty() {
            log::warn!(" empty IDs:");
            for (data_set_id, ids) in &self.empty_ids {
                let path = substrates
                    .get_path_for_id(*data_set_id)
                    .map_or_else(|| UNKNOWN.to_string(), |path| format!("{path:?}"));
                log::warn!("  - {}: {}", path, ids.len());
            }
        }
        if !self.missing_inner_ids.is_empty() {
            log::warn!(" missing inner IDs:");
            for (data_set_id, ids) in &self.missing_inner_ids {
                let path = substrates
                    .get_path_for_id(*data_set_id)
                    .map_or_else(|| UNKNOWN.to_string(), |path| format!("{path:?}"));
                log::warn!("  - {}: {}", path, ids.len());
            }
        }
        log::warn!("End of the report");
    }
}

/// An ID unique within a context inside of a single substrate file.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct InnerId(String);

impl InnerId {
    #[must_use]
    pub fn new(id: String) -> Self {
        Self(id)
    }

    #[must_use]
    pub fn to_str(&self) -> &str {
        &self.0
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
    pub fn to_error_not_found(
        &self,
        substrate: &Substrate,
        when: &str,
    ) -> errors::CrystalizationError {
        errors::CrystalizationError::UniqueIdNotFoundForInnerId {
            data_set_path: substrate.path.clone(),
            inner_id: self.inner.clone(),
            when: when.to_owned(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct RawExternalId(Vec<u8>);

impl AsRef<[u8]> for RawExternalId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl<'a> kv::Key<'a> for RawExternalId {
    fn from_raw_key(r: &'a kv::Raw) -> Result<Self, kv::Error> {
        Ok(Self(r.to_vec()))
    }
}

trait UniqueId: Clone + Eq + Ord + std::hash::Hash {
    fn zero() -> Self;
    fn increment(&mut self);
}

impl UniqueId for gather::OrganisationId {
    fn zero() -> Self {
        Self::from_value(0)
    }

    fn increment(&mut self) {
        *self = Self::from_value(self.get_value() + 1);
    }
}

impl UniqueId for gather::ProductId {
    fn zero() -> Self {
        Self::from_value(0)
    }

    fn increment(&mut self) {
        *self = Self::from_value(self.get_value() + 1);
    }
}

#[allow(dead_code)]
trait IndividualId: Clone + Eq + Ord + std::hash::Hash + std::fmt::Debug {}

trait IdStructure {
    type IndividualId: IndividualId;

    fn get_external_id(&self) -> ExternalId;
    fn get_individual_ids(&self) -> Vec<Self::IndividualId>;
}

#[derive(Clone, Debug)]
struct IdCombinationResult<U>
where
    U: UniqueId + std::fmt::Debug,
{
    pub num_empty_ids: u64,
    pub num_unique_ids: u64,
    pub external_to_unique: BTreeMap<ExternalId, U>,
}

impl<U> Default for IdCombinationResult<U>
where
    U: UniqueId + std::fmt::Debug,
{
    fn default() -> Self {
        Self { num_empty_ids: 0, num_unique_ids: 0, external_to_unique: BTreeMap::new() }
    }
}

#[must_use]
#[derive(Debug)]
struct IdMap<U>
where
    U: UniqueId,
{
    external_to_unique: BTreeMap<ExternalId, U>,
}

impl<U> IdMap<U>
where
    U: UniqueId,
{
    fn get_unique_for_external(&self, external_id: &ExternalId) -> Result<U, ExternalId> {
        if let Some(unique_id) = self.external_to_unique.get(external_id) {
            Ok(unique_id.clone())
        } else {
            Err(external_id.clone())
        }
    }
}

pub struct Bucket<'a, K, V> {
    bucket: kv::Bucket<'a, Vec<u8>, Vec<u8>>,
    phantom: std::marker::PhantomData<(K, V)>,
}

impl<'a, K, V> Bucket<'a, K, V> {
    #[must_use]
    pub fn len(&self) -> usize {
        self.bucket.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bucket.is_empty()
    }

    pub fn get(&self, key: &K) -> Result<Option<V>, errors::KvStoreError>
    where
        K: Serialize,
        V: DeserializeOwned,
    {
        let key_data = postcard::to_stdvec(key)?;
        let value_data = self.bucket.get(&key_data)?;
        Ok(if let Some(value_data) = value_data {
            Some(postcard::from_bytes(&value_data)?)
        } else {
            None
        })
    }

    pub fn remove(&self, key: &K) -> Result<Option<V>, errors::KvStoreError>
    where
        K: Serialize,
        V: DeserializeOwned,
    {
        let key_data = postcard::to_stdvec(key)?;
        let value_data = self.bucket.remove(&key_data)?;
        Ok(if let Some(value_data) = value_data {
            Some(postcard::from_bytes(&value_data)?)
        } else {
            None
        })
    }

    pub fn insert(&self, key: &K, value: &V) -> Result<(), errors::KvStoreError>
    where
        K: Serialize,
        V: Serialize,
    {
        let key_data = postcard::to_stdvec(key)?;
        let value_data = postcard::to_stdvec(value)?;
        self.bucket.set(&key_data, &value_data)?;
        Ok(())
    }
}

struct KvStore {
    store: kv::Store,
}

impl KvStore {
    pub fn new(path: &std::path::Path) -> Result<Self, errors::KvStoreError> {
        Ok(Self { store: kv::Store::new(kv::Config::new(path))? })
    }

    pub fn get_producer_external_to_individuals_bucket(
        &self,
    ) -> Result<Bucket<ExternalId, Vec<IndividualProducerId>>, errors::KvStoreError> {
        let bucket =
            self.store.bucket::<Vec<u8>, Vec<u8>>(Some("producers_external_to_individuals"))?;
        Ok(Bucket { bucket, phantom: std::marker::PhantomData })
    }

    pub fn get_producer_individual_to_externals_bucket(
        &self,
    ) -> Result<Bucket<IndividualProducerId, Vec<ExternalId>>, errors::KvStoreError> {
        let bucket =
            self.store.bucket::<Vec<u8>, Vec<u8>>(Some("producers_individual_to_externals"))?;
        Ok(Bucket { bucket, phantom: std::marker::PhantomData })
    }

    pub fn get_product_external_to_individuals_bucket(
        &self,
    ) -> Result<Bucket<ExternalId, Vec<IndividualProductId>>, errors::KvStoreError> {
        let bucket =
            self.store.bucket::<Vec<u8>, Vec<u8>>(Some("products_external_to_individuals"))?;
        Ok(Bucket { bucket, phantom: std::marker::PhantomData })
    }

    pub fn get_product_individual_to_externals_bucket(
        &self,
    ) -> Result<Bucket<IndividualProductId, Vec<ExternalId>>, errors::KvStoreError> {
        let bucket =
            self.store.bucket::<Vec<u8>, Vec<u8>>(Some("products_individual_to_externals"))?;
        Ok(Bucket { bucket, phantom: std::marker::PhantomData })
    }
}

struct IdCombiner<'a, T, I, U>
where
    T: IdStructure<IndividualId = I>,
    I: IndividualId + Serialize + DeserializeOwned + 'static,
    U: UniqueId + std::fmt::Debug,
{
    /// Mapping from external to individual IDs.
    external_to_individuals: Bucket<'a, ExternalId, Vec<I>>,

    /// Mapping from individual to external IDs.
    individual_to_externals: Bucket<'a, I, Vec<ExternalId>>,

    /// The result to return from `combine`.
    result: IdCombinationResult<U>,

    phantom: std::marker::PhantomData<T>,
}

impl<'a, T, I, U> IdCombiner<'a, T, I, U>
where
    T: IdStructure<IndividualId = I> + std::fmt::Debug,
    I: IndividualId + Serialize + DeserializeOwned + 'static,
    U: UniqueId + std::fmt::Debug,
{
    pub fn new(
        external_to_individuals: Bucket<'a, ExternalId, Vec<I>>,
        individual_to_externals: Bucket<'a, I, Vec<ExternalId>>,
    ) -> Self {
        Self {
            external_to_individuals,
            individual_to_externals,
            result: IdCombinationResult::default(),
            phantom: std::marker::PhantomData,
        }
    }

    pub fn combine(
        mut self,
        ids: &[T],
    ) -> Result<IdCombinationResult<U>, errors::CrystalizationError> {
        self.fill_ids(ids)?;
        self.find_clusters(ids)
    }

    fn fill_ids(&mut self, ids: &[T]) -> Result<(), errors::CrystalizationError> {
        for id in ids {
            let external_id = id.get_external_id();
            let individual_ids_vec = id.get_individual_ids();

            for individual_id in &individual_ids_vec {
                let insert = match self
                    .individual_to_externals
                    .get(individual_id)
                    .expect("Individual ID must exist")
                {
                    Some(mut entry) => {
                        entry.push(external_id.clone());
                        entry.sort();
                        entry.dedup();
                        entry
                    }
                    None => {
                        vec![external_id.clone()]
                    }
                };

                self.individual_to_externals.insert(individual_id, &insert)?;
            }

            self.external_to_individuals.insert(&external_id, &individual_ids_vec)?;
        }

        Ok(())
    }

    fn find_clusters(
        mut self,
        ids: &[T],
    ) -> Result<IdCombinationResult<U>, errors::CrystalizationError> {
        let mut unique_id = U::zero();

        for id in ids {
            let external_id = id.get_external_id();

            if self.result.external_to_unique.contains_key(&external_id) {
                // This ID was already processed
                continue;
            }

            unique_id.increment();
            self.result.num_unique_ids += 1;

            let mut individual_ids = id.get_individual_ids();
            if individual_ids.is_empty() {
                self.external_to_individuals.remove(&external_id)?;
                self.result.num_empty_ids += 1;
                self.result.external_to_unique.insert(external_id, unique_id.clone());
                continue;
            }

            loop {
                let external_ids = self.gather_external_ids(individual_ids, &unique_id)?;
                if external_ids.is_empty() {
                    break;
                }
                individual_ids = self.gather_individual_ids(external_ids)?;
                if individual_ids.is_empty() {
                    break;
                }
            }
        }

        Ok(self.result)
    }

    fn gather_external_ids(
        &mut self,
        individual_ids: Vec<I>,
        unique_id: &U,
    ) -> Result<BTreeSet<ExternalId>, errors::CrystalizationError> {
        let mut new_ids = BTreeSet::new();
        for individual_id in individual_ids {
            if let Some(external_ids) = self.individual_to_externals.remove(&individual_id)? {
                for external_id in external_ids {
                    let old = self
                        .result
                        .external_to_unique
                        .insert(external_id.clone(), unique_id.clone());
                    if old.is_none() {
                        new_ids.insert(external_id.clone());
                    }
                }
            }
        }
        Ok(new_ids)
    }

    fn gather_individual_ids(
        &mut self,
        external_ids: BTreeSet<ExternalId>,
    ) -> Result<Vec<I>, errors::CrystalizationError> {
        let mut new_ids = Vec::<I>::new();
        for external_id in external_ids {
            if let Some(internal_ids) = self.external_to_individuals.remove(&external_id)? {
                new_ids.extend_from_slice(&internal_ids);
            }
        }
        new_ids.sort();
        new_ids.dedup();
        Ok(new_ids)
    }
}

/// An ID identifying a producer in the source data.
///
/// One producer may have multiple individual IDs, but producers cannot share IDs.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
enum IndividualProducerId {
    /// VAT number.
    Vat(ids::VatId),

    /// Wikidata ID.
    Wiki(ids::WikiId),

    /// Web domains.
    // TODO: Introduce dedicated type.
    Domain(String),
}

impl IndividualId for IndividualProducerId {}

#[derive(Clone, Debug)]
pub struct ProducerIds {
    external: ExternalId,
    individual: Vec<IndividualProducerId>,
}

impl ProducerIds {
    #[must_use]
    pub fn from_catalog(
        producer: &schema::CatalogProducer,
        data_set_id: DataSetId,
    ) -> (Self, Vec<CrystalizationWarning>) {
        let external = ExternalId::new(data_set_id, InnerId::new(producer.id.clone()));
        let (individual, warnings) = Self::extract_individual_ids(&producer.ids, data_set_id);
        (Self { external, individual }, warnings)
    }

    #[must_use]
    pub fn from_review(
        producer: &schema::ReviewProducer,
        data_set_id: DataSetId,
    ) -> (Self, Vec<CrystalizationWarning>) {
        let external = ExternalId::new(data_set_id, InnerId::new(producer.id.clone()));
        let (individual, warnings) = Self::extract_individual_ids(&producer.ids, data_set_id);
        (Self { external, individual }, warnings)
    }

    #[must_use]
    fn extract_individual_ids(
        ids: &schema::ProducerIds,
        data_set_id: DataSetId,
    ) -> (Vec<IndividualProducerId>, Vec<CrystalizationWarning>) {
        let mut individual = Vec::new();
        let mut warnings = Vec::new();
        if let Some(vat) = &ids.vat {
            for id in vat {
                match ids::VatId::try_from(id) {
                    Ok(id) => individual.push(IndividualProducerId::Vat(id)),
                    Err(_) => warnings.push(CrystalizationWarning::InvalidIndividualId {
                        data_set_id,
                        individual_id: id.clone(),
                    }),
                }
            }
        }
        if let Some(wiki) = &ids.wiki {
            for id in wiki {
                match ids::WikiId::try_from(id) {
                    Ok(id) => individual.push(IndividualProducerId::Wiki(id)),
                    Err(_) => warnings.push(CrystalizationWarning::InvalidIndividualId {
                        data_set_id,
                        individual_id: id.clone(),
                    }),
                }
            }
        }
        if let Some(domains) = &ids.domains {
            for domain in domains {
                // TODO: validate the domains
                individual.push(IndividualProducerId::Domain(domain.clone()));
            }
        }

        (individual, warnings)
    }
}

impl IdStructure for ProducerIds {
    type IndividualId = IndividualProducerId;

    fn get_external_id(&self) -> ExternalId {
        self.external.clone()
    }

    fn get_individual_ids(&self) -> Vec<Self::IndividualId> {
        self.individual.clone()
    }
}

/// An ID identifying a product in the source data.
///
/// One product may have multiple individual IDs, but products cannot share IDs.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
enum IndividualProductId {
    /// EAN.
    Ean(String),

    /// GTIN.
    Gtin(String),

    /// Wikidata ID.
    Wiki(String),
}

impl IndividualId for IndividualProductId {}

#[derive(Clone, Debug)]
pub struct ProductIds {
    external: ExternalId,
    individual: Vec<IndividualProductId>,
}

impl ProductIds {
    #[must_use]
    pub fn from_catalog(
        product: &schema::CatalogProduct,
        data_set_id: DataSetId,
    ) -> (Self, Vec<CrystalizationWarning>) {
        let external = ExternalId::new(data_set_id, InnerId::new(product.id.clone()));
        let (individual, warnings) = Self::extract_individual_ids(&product.ids, data_set_id);
        (Self { external, individual }, warnings)
    }

    #[must_use]
    pub fn from_producer(
        product: &schema::ProducerProduct,
        data_set_id: DataSetId,
    ) -> (Self, Vec<CrystalizationWarning>) {
        let external = ExternalId::new(data_set_id, InnerId::new(product.id.clone()));
        let (individual, warnings) = Self::extract_individual_ids(&product.ids, data_set_id);
        (Self { external, individual }, warnings)
    }

    #[must_use]
    pub fn from_review(
        product: &schema::ReviewProduct,
        data_set_id: DataSetId,
    ) -> (Self, Vec<CrystalizationWarning>) {
        let external = ExternalId::new(data_set_id, InnerId::new(product.id.clone()));
        let (individual, warnings) = Self::extract_individual_ids(&product.ids, data_set_id);
        (Self { external, individual }, warnings)
    }

    fn extract_individual_ids(
        ids: &schema::ProductIds,
        data_set_id: DataSetId,
    ) -> (Vec<IndividualProductId>, Vec<CrystalizationWarning>) {
        let mut individual = Vec::new();
        let mut warnings = Vec::new();
        if let Some(ean) = &ids.ean {
            for id in ean {
                match ids::Ean::try_from(id) {
                    Ok(id) => individual.push(IndividualProductId::Ean(id.to_canonical_string())),
                    Err(_) => warnings.push(CrystalizationWarning::InvalidIndividualId {
                        data_set_id,
                        individual_id: id.clone(),
                    }),
                }
            }
        }
        if let Some(gtin) = &ids.gtin {
            for id in gtin {
                match ids::Gtin::try_from(id) {
                    Ok(id) => individual.push(IndividualProductId::Gtin(id.to_canonical_string())),
                    Err(_) => warnings.push(CrystalizationWarning::InvalidIndividualId {
                        data_set_id,
                        individual_id: id.clone(),
                    }),
                }
            }
        }
        if let Some(wiki) = &ids.wiki {
            for id in wiki {
                match ids::WikiId::try_from(id) {
                    Ok(id) => individual.push(IndividualProductId::Wiki(id.to_canonical_string())),
                    Err(_) => warnings.push(CrystalizationWarning::InvalidIndividualId {
                        data_set_id,
                        individual_id: id.clone(),
                    }),
                }
            }
        }

        (individual, warnings)
    }
}

impl IdStructure for ProductIds {
    type IndividualId = IndividualProductId;

    fn get_external_id(&self) -> ExternalId {
        self.external.clone()
    }

    fn get_individual_ids(&self) -> Vec<Self::IndividualId> {
        self.individual.clone()
    }
}

/// ID maps for producers and products.
#[must_use]
pub struct GroupedIds {
    /// Producer ID map.
    producer: IdMap<gather::OrganisationId>,

    /// Product ID map.
    product: IdMap<gather::ProductId>,
}

impl GroupedIds {
    fn get_unique_id_for_producer_external_id(
        &self,
        external_producer_id: &ExternalId,
    ) -> Result<gather::OrganisationId, ExternalId> {
        self.producer.get_unique_for_external(external_producer_id)
    }

    fn get_unique_id_for_product_external_id(
        &self,
        external_product_id: &ExternalId,
    ) -> Result<gather::ProductId, ExternalId> {
        self.product.get_unique_for_external(external_product_id)
    }
}

/// Data storage for gathered data.
///
/// Allows merging different instances.
#[derive(Debug, Default, Clone)]
pub struct CrystalizationCollector {
    /// Found organisations.
    organisations: BTreeMap<gather::OrganisationId, gather::Organisation>,

    /// Found products.
    products: BTreeMap<gather::ProductId, gather::Product>,
}

impl CrystalizationCollector {
    pub fn update_organisation(
        &mut self,
        id: gather::OrganisationId,
        organisation: gather::Organisation,
    ) {
        match self.organisations.entry(id) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().merge(organisation);
            }
            Entry::Vacant(entry) => {
                entry.insert(organisation);
            }
        }
    }

    pub fn update_product(&mut self, id: gather::ProductId, product: gather::Product) {
        match self.products.entry(id) {
            Entry::Occupied(mut entry) => entry.get_mut().merge(product),
            Entry::Vacant(entry) => {
                let _ = entry.insert(product);
            }
        }
    }
}

#[derive(Default)]
pub struct Summary {
    pub product_ids: Vec<ProductIds>,
    pub producer_ids: Vec<ProducerIds>,
}

#[derive(Debug, derive_new::new)]
pub struct Grouper {}

impl Grouper {
    fn group(
        substrates: &Substrates,
        config: &config::CrystalizationConfig,
    ) -> Result<(GroupedIds, CrystalizationReport), errors::CrystalizationError> {
        log::info!("Gathering IDs");
        let (summary, report) = Self::summarize(substrates)?;

        if config.local_storage_runtime.exists() {
            std::fs::remove_dir_all(&config.local_storage_runtime).map_err(|e| {
                errors::CrystalizationError::Io(e, config.local_storage_runtime.clone())
            })?;
        }
        let store = KvStore::new(&config.local_storage_runtime)?;

        log::info!("Grouping producer IDs");
        let producer = {
            let combiner =
                IdCombiner::<ProducerIds, IndividualProducerId, gather::OrganisationId>::new(
                    store.get_producer_external_to_individuals_bucket()?,
                    store.get_producer_individual_to_externals_bucket()?,
                );
            let result = combiner.combine(&summary.producer_ids)?;

            log::info!("Producers:");
            log::info!(" - unique IDs: {}", result.num_unique_ids);
            log::info!(" - empty IDs: {}", result.num_empty_ids);

            IdMap { external_to_unique: result.external_to_unique }
        };

        log::info!("Grouping product IDs");
        let product = {
            let combiner = IdCombiner::<ProductIds, IndividualProductId, gather::ProductId>::new(
                store.get_product_external_to_individuals_bucket()?,
                store.get_product_individual_to_externals_bucket()?,
            );
            let result = combiner.combine(&summary.product_ids)?;

            log::info!("Products:");
            log::info!(" - unique IDs: {}", result.num_unique_ids);
            log::info!(" - empty IDs: {}", result.num_empty_ids);

            IdMap { external_to_unique: result.external_to_unique }
        };

        Ok((GroupedIds { producer, product }, report))
    }

    pub fn summarize(
        substrates: &Substrates,
    ) -> Result<(Summary, CrystalizationReport), errors::CrystalizationError> {
        let mut result = Summary::default();
        let mut report = CrystalizationReport::default();
        for substrate in substrates.list() {
            match schema::read::iter_file(&substrate.path)? {
                schema::read::FileIterVariant::Catalog(iter) => {
                    for entry in iter {
                        match entry? {
                            schema::CatalogEntry::Producer(producer) => {
                                let (ids, warnings) =
                                    ProducerIds::from_catalog(&producer, substrate.id);
                                result.producer_ids.push(ids);
                                report.add_many(warnings);
                            }
                            schema::CatalogEntry::Product(product) => {
                                let (ids, warnings) =
                                    ProductIds::from_catalog(&product, substrate.id);
                                result.product_ids.push(ids);
                                report.add_many(warnings);
                            }
                        }
                    }
                }
                schema::read::FileIterVariant::Producer(iter) => {
                    for entry in iter {
                        match entry? {
                            schema::ProducerEntry::Product(product) => {
                                let (ids, warnings) =
                                    ProductIds::from_producer(&product, substrate.id);
                                result.product_ids.push(ids);
                                report.add_many(warnings);
                            }
                            schema::ProducerEntry::Reviewer(_reviewer) => {
                                // this part of the data does not contain IDs
                            }
                        }
                    }
                }
                schema::read::FileIterVariant::Review(iter) => {
                    for entry in iter {
                        match entry? {
                            schema::ReviewEntry::Producer(producer) => {
                                let (ids, warnings) =
                                    ProducerIds::from_review(&producer, substrate.id);
                                result.producer_ids.push(ids);
                                report.add_many(warnings);
                            }
                            schema::ReviewEntry::Product(product) => {
                                let (ids, warnings) =
                                    ProductIds::from_review(&product, substrate.id);
                                result.product_ids.push(ids);
                                report.add_many(warnings);
                            }
                        }
                    }
                }
            }
        }

        Ok((result, report))
    }
}

#[derive(Debug, derive_new::new)]
pub struct Processor {
    /// Collected data.
    #[new(default)]
    collector: CrystalizationCollector,

    /// Report listing warnings from substrate files.
    #[new(default)]
    report: CrystalizationReport,
}

impl Processor {
    fn process(
        mut self,
        substrates: &Substrates,
        groups: &GroupedIds,
    ) -> Result<(CrystalizationCollector, CrystalizationReport), errors::CrystalizationError> {
        log::info!("Processing data");
        for substrate in substrates.list() {
            match schema::read::iter_file(&substrate.path)? {
                schema::read::FileIterVariant::Catalog(iter) => {
                    for entry in iter {
                        match entry? {
                            schema::CatalogEntry::Producer(producer) => {
                                self.process_catalog_producer(producer, groups, substrate)?;
                            }
                            schema::CatalogEntry::Product(product) => {
                                self.process_catalog_product(product, groups, substrate)?;
                            }
                        }
                    }
                }
                schema::read::FileIterVariant::Producer(iter) => {
                    for entry in iter {
                        match entry? {
                            schema::ProducerEntry::Product(product) => {
                                self.process_producer_product(product, groups, substrate)?;
                            }
                            schema::ProducerEntry::Reviewer(_reviewer) => {
                                // TODO: use the reviewer data
                            }
                        }
                    }
                }
                schema::read::FileIterVariant::Review(iter) => {
                    for entry in iter {
                        match entry? {
                            schema::ReviewEntry::Producer(producer) => {
                                self.process_review_producer(producer, groups, substrate)?;
                            }
                            schema::ReviewEntry::Product(product) => {
                                self.process_review_product(product, groups, substrate)?;
                            }
                        }
                    }
                }
            }
        }
        Ok((self.collector, self.report))
    }

    fn process_catalog_producer(
        &mut self,
        producer: schema::CatalogProducer,
        groups: &GroupedIds,
        substrate: &Substrate,
    ) -> Result<(), errors::CrystalizationError> {
        let external_id = ExternalId::new(substrate.id, InnerId::new(producer.id));
        let unique_id = groups
            .get_unique_id_for_producer_external_id(&external_id)
            .map_err(|id| id.to_error_not_found(substrate, "processing catalog producer"))?;
        let ids = self.convert_organisation_ids(producer.ids, substrate);

        self.collector.update_organisation(
            unique_id.clone(),
            gather::Organisation {
                db_key: unique_id,
                ids,
                names: producer
                    .names
                    .into_iter()
                    .map(|text| gather::Text { text, source: substrate.source.clone() })
                    .collect(),
                descriptions: producer
                    .description
                    .into_iter()
                    .map(|text| gather::Text { text, source: substrate.source.clone() })
                    .collect(),
                images: producer
                    .images
                    .into_iter()
                    .map(|image| gather::Image { image, source: substrate.source.clone() })
                    .collect(),
                websites: producer.websites.into_iter().collect(),
                certifications: gather::Certifications::default(),
            },
        );

        Ok(())
    }

    fn process_catalog_product(
        &mut self,
        product: schema::CatalogProduct,
        groups: &GroupedIds,
        substrate: &Substrate,
    ) -> Result<(), errors::CrystalizationError> {
        let external_id = ExternalId::new(substrate.id, InnerId::new(product.id));
        let unique_id = groups
            .get_unique_id_for_product_external_id(&external_id)
            .map_err(|id| id.to_error_not_found(substrate, "processing catalog product"))?;
        let ids = self.convert_product_ids(product.ids, substrate);
        let (followed_by, follows) =
            self.extract_related_products(&product.related, groups, substrate);
        let manufacturer_ids = self.extract_manufacturer_ids(&product.origins, groups, substrate);

        self.collector.update_product(
            unique_id.clone(),
            gather::Product {
                db_key: unique_id,
                ids,
                names: product
                    .names
                    .into_iter()
                    .map(|text| gather::Text { text, source: substrate.source.clone() })
                    .collect(),
                descriptions: product
                    .description
                    .into_iter()
                    .map(|text| gather::Text { text, source: substrate.source.clone() })
                    .collect(),
                images: product
                    .images
                    .into_iter()
                    .map(|image| gather::Image { image, source: substrate.source.clone() })
                    .collect(),
                categories: product.categorisation.map_or_else(BTreeSet::new, |c| {
                    c.categories.iter().map(|c| c.join("/")).collect()
                }),
                regions: extract_regions(product.availability.as_ref())?,
                manufacturer_ids,
                follows,
                followed_by,
                sustainity_score: gather::SustainityScore::default(), //< Calculated later
                certifications: gather::Certifications::default(),
            },
        );

        Ok(())
    }

    fn process_producer_product(
        &mut self,
        product: schema::ProducerProduct,
        groups: &GroupedIds,
        substrate: &Substrate,
    ) -> Result<(), errors::CrystalizationError> {
        let external_id = ExternalId::new(substrate.id, InnerId::new(product.id));
        let unique_id = groups
            .get_unique_id_for_product_external_id(&external_id)
            .map_err(|id| id.to_error_not_found(substrate, "processing producer product"))?;
        let ids = self.convert_product_ids(product.ids, substrate);
        let (followed_by, follows) =
            self.extract_related_products(&product.related, groups, substrate);
        let manufacturer_ids = self.extract_manufacturer_ids(&product.origins, groups, substrate);

        self.collector.update_product(
            unique_id.clone(),
            gather::Product {
                db_key: unique_id,
                ids,
                names: product
                    .names
                    .into_iter()
                    .map(|text| gather::Text { text, source: substrate.source.clone() })
                    .collect(),
                descriptions: BTreeSet::new(),
                images: product
                    .images
                    .into_iter()
                    .map(|image| gather::Image { image, source: substrate.source.clone() })
                    .collect(),
                categories: product.categorisation.categories.iter().map(|c| c.join("/")).collect(),
                regions: extract_regions(product.availability.as_ref())?,
                manufacturer_ids,
                follows,
                followed_by,
                sustainity_score: gather::SustainityScore::default(), //< Calculated later
                certifications: gather::Certifications::default(),
            },
        );

        Ok(())
    }

    fn process_review_producer(
        &mut self,
        producer: schema::ReviewProducer,
        groups: &GroupedIds,
        substrate: &Substrate,
    ) -> Result<(), errors::CrystalizationError> {
        let certifications = gather::Certifications {
            bcorp: Self::extract_bcorp_cert(&producer, substrate),
            eu_ecolabel: Self::extract_euecolabel_cert(substrate),
            fti: Self::extract_fti_cert(&producer, substrate),
            tco: Self::extract_tco_cert(&producer, substrate),
        };

        let external_id = ExternalId::new(substrate.id, InnerId::new(producer.id.clone()));
        let unique_id = groups
            .get_unique_id_for_producer_external_id(&external_id)
            .map_err(|id| id.to_error_not_found(substrate, "processing review producer"))?;
        let ids = self.convert_organisation_ids(producer.ids, substrate);

        self.collector.update_organisation(
            unique_id.clone(),
            gather::Organisation {
                db_key: unique_id,
                ids,
                names: producer
                    .names
                    .into_iter()
                    .map(|text| gather::Text { text, source: substrate.source.clone() })
                    .collect(),
                descriptions: producer
                    .description
                    .into_iter()
                    .map(|text| gather::Text { text, source: substrate.source.clone() })
                    .collect(),
                images: producer
                    .images
                    .into_iter()
                    .map(|image| gather::Image { image, source: substrate.source.clone() })
                    .collect(),
                websites: producer.websites.into_iter().collect(),
                certifications,
            },
        );

        Ok(())
    }

    fn process_review_product(
        &mut self,
        product: schema::ReviewProduct,
        groups: &GroupedIds,
        substrate: &Substrate,
    ) -> Result<(), errors::CrystalizationError> {
        let external_id = ExternalId::new(substrate.id, InnerId::new(product.id));
        let unique_id = groups
            .get_unique_id_for_product_external_id(&external_id)
            .map_err(|id| id.to_error_not_found(substrate, "processing review product"))?;
        let ids = self.convert_product_ids(product.ids, substrate);
        let (followed_by, follows) =
            self.extract_related_products(&product.related, groups, substrate);
        let manufacturer_ids = self.extract_manufacturer_ids(&product.origins, groups, substrate);

        self.collector.update_product(
            unique_id.clone(),
            gather::Product {
                db_key: unique_id,
                ids,
                names: product
                    .names
                    .into_iter()
                    .map(|text| gather::Text { text, source: substrate.source.clone() })
                    .collect(),
                descriptions: BTreeSet::new(),
                images: product
                    .images
                    .into_iter()
                    .map(|image| gather::Image { image, source: substrate.source.clone() })
                    .collect(),
                categories: product.categorisation.map_or_else(BTreeSet::new, |c| {
                    c.categories.iter().map(|c| c.join("/")).collect()
                }),
                regions: extract_regions(product.availability.as_ref())?,
                manufacturer_ids,
                follows,
                followed_by,
                sustainity_score: gather::SustainityScore::default(), //< Calculated later
                certifications: gather::Certifications::default(), //< Assigned later from producers
            },
        );

        Ok(())
    }

    #[allow(clippy::unused_self)]
    fn extract_manufacturer_ids(
        &mut self,
        origins: &Option<schema::ProductOrigins>,
        groups: &GroupedIds,
        substrate: &Substrate,
    ) -> BTreeSet<gather::OrganisationId> {
        let mut manufacturer_ids = BTreeSet::new();
        if let Some(origins) = &origins {
            for producer_id in &origins.producer_ids {
                let external_id = ExternalId::new(substrate.id, InnerId::new(producer_id.clone()));
                match groups.get_unique_id_for_producer_external_id(&external_id) {
                    Ok(unique_id) => {
                        manufacturer_ids.insert(unique_id);
                    }
                    Err(external_id) => self.report.add_missing_external_id(external_id),
                }
            }
        }
        manufacturer_ids
    }

    fn extract_related_products(
        &mut self,
        related: &Option<schema::RelatedProducts>,
        groups: &GroupedIds,
        substrate: &Substrate,
    ) -> (BTreeSet<gather::ProductId>, BTreeSet<gather::ProductId>) {
        let mut follows = BTreeSet::new();
        let mut followed_by = BTreeSet::new();
        if let Some(related) = related {
            if let Some(precedents) = &related.preceded_by {
                follows = self.convert_inner_ids(precedents, groups, substrate);
            }
            if let Some(followers) = &related.followed_by {
                followed_by = self.convert_inner_ids(followers, groups, substrate);
            }
        }
        (follows, followed_by)
    }

    fn convert_inner_ids(
        &mut self,
        input: &[String],
        groups: &GroupedIds,
        substrate: &Substrate,
    ) -> BTreeSet<gather::ProductId> {
        let mut result = BTreeSet::new();
        for product_id in input {
            let external_id = ExternalId::new(substrate.id, InnerId::new(product_id.clone()));
            match groups.get_unique_id_for_product_external_id(&external_id) {
                Ok(unique_id) => {
                    result.insert(unique_id);
                }
                Err(external_id) => self.report.add_missing_external_id(external_id),
            }
        }
        result
    }

    fn extract_bcorp_cert(
        producer: &schema::ReviewProducer,
        substrate: &Substrate,
    ) -> Option<gather::BCorpCert> {
        if !substrate.source.is_bcorp() {
            return None;
        }

        Some(gather::BCorpCert { id: producer.id.clone() })
    }

    fn extract_euecolabel_cert(substrate: &Substrate) -> Option<gather::EuEcolabelCert> {
        if !substrate.source.is_euecolabel() {
            return None;
        }

        Some(gather::EuEcolabelCert {})
    }

    fn extract_fti_cert(
        producer: &schema::ReviewProducer,
        substrate: &Substrate,
    ) -> Option<gather::FtiCert> {
        if !substrate.source.is_fti() {
            return None;
        }

        match &producer.review {
            Some(schema::Review::ScoreReview(review)) => {
                Some(gather::FtiCert { score: review.value })
            }
            _ => None,
        }
    }

    fn extract_tco_cert(
        producer: &schema::ReviewProducer,
        substrate: &Substrate,
    ) -> Option<gather::TcoCert> {
        if !substrate.source.is_tco() {
            return None;
        }

        // TODO: which name to pick?
        producer.names.first().cloned().map(|brand_name| gather::TcoCert { brand_name })
    }

    fn convert_product_ids(
        &mut self,
        ids: schema::ProductIds,
        substrate: &Substrate,
    ) -> gather::ProductIds {
        let mut eans = BTreeSet::<gather::Ean>::new();
        if let Some(ids) = ids.ean {
            for id in ids {
                match gather::Ean::try_from(&id) {
                    Ok(ean) => {
                        eans.insert(ean);
                    }
                    Err(_) => self.report.add_invalid_id(substrate.id, id),
                }
            }
        }

        let mut gtins = BTreeSet::<gather::Gtin>::new();
        if let Some(ids) = ids.gtin {
            for id in ids {
                match gather::Gtin::try_from(&id) {
                    Ok(gtin) => {
                        gtins.insert(gtin);
                    }
                    Err(_) => self.report.add_invalid_id(substrate.id, id),
                }
            }
        }

        let mut wiki = BTreeSet::<gather::WikiId>::new();
        if let Some(ids) = ids.wiki {
            for id in ids {
                match gather::WikiId::try_from(&id) {
                    Ok(wiki_id) => {
                        wiki.insert(wiki_id);
                    }
                    Err(_) => self.report.add_invalid_id(substrate.id, id),
                }
            }
        }

        gather::ProductIds { eans, gtins, wiki }
    }

    fn convert_organisation_ids(
        &mut self,
        ids: schema::ProducerIds,
        substrate: &Substrate,
    ) -> gather::OrganisationIds {
        let mut vat_ids = BTreeSet::<gather::VatId>::new();
        if let Some(ids) = ids.vat {
            for id in ids {
                match gather::VatId::try_from(&id) {
                    Ok(vat) => {
                        vat_ids.insert(vat);
                    }
                    Err(_) => self.report.add_invalid_id(substrate.id, id),
                }
            }
        }

        let mut wiki = BTreeSet::<gather::WikiId>::new();
        if let Some(ids) = ids.wiki {
            for id in ids {
                match gather::WikiId::try_from(&id) {
                    Ok(wiki_id) => {
                        wiki.insert(wiki_id);
                    }
                    Err(_) => self.report.add_invalid_id(substrate.id, id),
                }
            }
        }

        let mut domains = BTreeSet::<gather::Domain>::new();
        if let Some(ids) = ids.domains {
            for domain in ids {
                domains.insert(domain);
            }
        }

        gather::OrganisationIds { vat_ids, wiki, domains }
    }
}

#[derive(Debug, derive_new::new)]
pub struct Saver {
    /// Target configuration.
    config: config::TargetConfig,
}

impl Saver {
    /// Extracts keywords for DB text search from passed texts.
    fn extract_keywords(texts: &BTreeSet<gather::Text>) -> BTreeSet<String> {
        let mut result = BTreeSet::new();
        for text in texts {
            for word in text.text.split_whitespace() {
                result.insert(word.to_lowercase());
            }
        }
        result.remove("");
        result
    }

    fn finalize(
        organisations: &BTreeMap<gather::OrganisationId, gather::Organisation>,
        products: &mut BTreeMap<gather::ProductId, gather::Product>,
    ) {
        log::info!("Finalizing products");

        // Assign certifications to products
        log::info!(" -> assigning certifications");
        for product in products.values_mut() {
            for manufacturer_id in &product.manufacturer_ids {
                if let Some(organisation) = organisations.get(manufacturer_id) {
                    product.certifications.inherit(&organisation.certifications);
                }
                // TODO: There are many organisations that cannot be found.
                //       It seems like all of them are bugs in Wikidata.
                //       Make sure all organisations are found.
            }
        }

        // Calculate product Sustainity score
        log::info!(" -> calculating Sustainity scores");
        for product in products.values_mut() {
            product.sustainity_score = crate::score::calculate(product);
        }
    }

    /// Runs a quick sanity check: the `unique` should contain as many elements as `all`.
    fn uniqueness_check<T1, T2>(
        unique: &HashSet<T1>,
        all: &[T2],
        comment: &'static str,
    ) -> Result<(), errors::CrystalizationError> {
        if unique.len() == all.len() {
            Ok(())
        } else {
            Err(errors::CrystalizationError::NotUniqueKeys {
                comment: comment.to_string(),
                unique: unique.len(),
                all: all.len(),
            })
        }
    }

    /// Prepares organsation data.
    ///
    /// - fills left-over certifications
    /// - converts into a vector
    fn prepare_organisations(
        organisations: BTreeMap<gather::OrganisationId, gather::Organisation>,
    ) -> Vec<store::Organisation> {
        log::info!("Preparing organisations");

        organisations.into_values().map(|o| o.clone().store()).collect()
    }

    /// Prepares organsation keywords data.
    ///
    /// This data is needed to implement an efficient text search index.
    /// Data is composed from keyword vertex collection and edge collection connecting them to organisations.
    fn prepare_organisation_keywords(
        organisations: &BTreeMap<gather::OrganisationId, gather::Organisation>,
    ) -> Result<(Vec<gather::Keyword>, Vec<gather::Edge>), errors::CrystalizationError> {
        const COMMENT: &str = "organisation keywords";

        log::info!("Preparing {COMMENT}");

        let mut keywords = BTreeMap::<String, BTreeSet<gather::OrganisationId>>::new();
        for organisation in organisations.values() {
            for keyword in Self::extract_keywords(&organisation.names) {
                keywords
                    .entry(keyword)
                    .and_modify(|ids| {
                        ids.insert(organisation.db_key.clone());
                    })
                    .or_insert_with(|| [organisation.db_key.clone()].into());
            }
        }

        let mut uniqueness_check = HashSet::new();
        let mut organisation_keywords = Vec::<gather::Keyword>::with_capacity(keywords.len());
        let mut organisation_keyword_edges = Vec::<gather::Edge>::new();
        for (keyword, organisation_ids) in keywords {
            let ki = collections::organisation_keyword(&keyword);
            uniqueness_check.insert(ki.key.clone());
            organisation_keywords
                .push(gather::Keyword { db_key: ki.key, keyword: keyword.clone() });
            for organisation_id in organisation_ids {
                organisation_keyword_edges.push(gather::Edge {
                    from: ki.id.clone(),
                    to: collections::organisation(&organisation_id).id,
                });
            }
        }

        // Sanity check: all keys should be unique
        Self::uniqueness_check(&uniqueness_check, &organisation_keywords, COMMENT)?;

        Ok((organisation_keywords, organisation_keyword_edges))
    }

    /// Prepares VAT data.
    ///
    /// This data is needed to implement an efficient VAT search index.
    /// Data is composed from VAT vertex collection and edge collection connecting them to organisation.
    fn prepare_organisation_vat_ids(
        organisations: &BTreeMap<gather::OrganisationId, gather::Organisation>,
    ) -> Result<(Vec<gather::IdEntry>, Vec<gather::Edge>), errors::CrystalizationError> {
        const COMMENT: &str = "organisation VAT IDs";

        log::info!("Preparing {COMMENT}");
        let mut uniqueness_check = HashSet::new();
        let mut ids = Vec::<gather::IdEntry>::new();
        let mut id_edges = Vec::<gather::Edge>::new();
        for organisation in organisations.values() {
            for id in &organisation.ids.vat_ids {
                let vat_ki = collections::organisation_vat(id);
                let organisation_ki = collections::organisation(&organisation.db_key);
                uniqueness_check.insert(vat_ki.key.clone());
                ids.push(gather::IdEntry { db_key: vat_ki.key });
                id_edges.push(gather::Edge { from: vat_ki.id, to: organisation_ki.id });
            }
        }

        // Sanity check: all keys should be unique
        Self::uniqueness_check(&uniqueness_check, &ids, COMMENT)?;

        Ok((ids, id_edges))
    }

    /// Prepares Wikidata ID data.
    ///
    /// This data is needed to implement an efficient Wikidata ID search index.
    /// Data is composed from Wikidata ID vertex collection and edge collection connecting them to organisations.
    fn prepare_organisation_wiki_ids(
        organisations: &BTreeMap<gather::OrganisationId, gather::Organisation>,
    ) -> Result<(Vec<gather::IdEntry>, Vec<gather::Edge>), errors::CrystalizationError> {
        const COMMENT: &str = "organisation Wiki IDs";

        log::info!("Preparing {COMMENT}");

        let mut uniqueness_check = HashSet::new();
        let mut ids = Vec::<gather::IdEntry>::new();
        let mut id_edges = Vec::<gather::Edge>::new();
        for organisation in organisations.values() {
            for id in &organisation.ids.wiki {
                let wiki_ki = collections::organisation_wiki(id);
                let organisation_ki = collections::organisation(&organisation.db_key);
                uniqueness_check.insert(wiki_ki.key.clone());
                ids.push(gather::IdEntry { db_key: wiki_ki.key });
                id_edges.push(gather::Edge { from: wiki_ki.id, to: organisation_ki.id });
            }
        }

        // Sanity check: all keys should be unique
        Self::uniqueness_check(&uniqueness_check, &ids, COMMENT)?;

        Ok((ids, id_edges))
    }

    /// Prepares organisation WWW domain data.
    ///
    /// This data is needed to implement an efficient WWW domain search index.
    /// Data is composed from WWW domain vertex collection and edge collection connecting them to organisations.
    fn prepare_organisation_wwws(
        organisations: &BTreeMap<gather::OrganisationId, gather::Organisation>,
    ) -> Result<(Vec<gather::IdEntry>, Vec<gather::Edge>), errors::CrystalizationError> {
        const COMMENT: &str = "organisation WWW domains";

        log::info!("Preparing {COMMENT}");

        let mut uniqueness_check = HashSet::new();
        let mut ids = Vec::<gather::IdEntry>::new();
        let mut id_edges = Vec::<gather::Edge>::new();
        for organisation in organisations.values() {
            for id in &organisation.ids.domains {
                let www_ki = collections::organisation_www(id);
                let organisation_ki = collections::organisation(&organisation.db_key);
                uniqueness_check.insert(www_ki.key.clone());
                ids.push(gather::IdEntry { db_key: www_ki.key });
                id_edges.push(gather::Edge { from: www_ki.id, to: organisation_ki.id });
            }
        }

        // Sanity check: all keys should be unique
        Self::uniqueness_check(&uniqueness_check, &ids, "COMMENT")?;

        Ok((ids, id_edges))
    }

    /// Prepares product data.
    ///
    /// - fills left-over certifications
    /// - converts into a vector
    fn prepare_products(
        products: BTreeMap<gather::ProductId, gather::Product>,
    ) -> Vec<store::Product> {
        log::info!("Preparing products");

        products.into_values().map(|p| p.clone().store()).collect()
    }

    /// Prepares product keywords data.
    ///
    /// This data is needed to implement an efficient text search index.
    /// Data is composed from keyword vertex collection and edge collection connecting them to products.
    fn prepare_product_keywords(
        products: &BTreeMap<gather::ProductId, gather::Product>,
    ) -> Result<(Vec<gather::Keyword>, Vec<gather::Edge>), errors::CrystalizationError> {
        const COMMENT: &str = "product keywords";

        log::info!("Preparing {COMMENT}");

        let mut keywords = BTreeMap::<String, BTreeSet<gather::ProductId>>::new();
        for product in products.values() {
            for keyword in Self::extract_keywords(&product.names) {
                keywords
                    .entry(keyword)
                    .and_modify(|ids| {
                        ids.insert(product.db_key.clone());
                    })
                    .or_insert_with(|| [product.db_key.clone()].into());
            }
        }

        let mut uniqueness_check = HashSet::new();
        let mut product_keywords = Vec::<gather::Keyword>::with_capacity(keywords.len());
        let mut product_keyword_edges = Vec::<gather::Edge>::new();
        for (keyword, product_ids) in keywords {
            let product_keyword_ki = collections::product_keyword(&keyword);
            uniqueness_check.insert(product_keyword_ki.key.clone());
            product_keywords
                .push(gather::Keyword { db_key: product_keyword_ki.key, keyword: keyword.clone() });
            for product_id in product_ids {
                let product_ki = collections::product(&product_id);
                product_keyword_edges
                    .push(gather::Edge { from: product_keyword_ki.id.clone(), to: product_ki.id });
            }
        }

        // Sanity check: all keys should be unique
        Self::uniqueness_check(&uniqueness_check, &product_keywords, COMMENT)?;

        Ok((product_keywords, product_keyword_edges))
    }

    /// Prepares EAN data.
    ///
    /// This data is needed to implement an efficient EAN search index.
    /// Data is composed from EAN vertex collection and edge collection connecting them to products.
    fn prepare_eans(
        products: &BTreeMap<gather::ProductId, gather::Product>,
    ) -> Result<(Vec<gather::IdEntry>, Vec<gather::Edge>), errors::CrystalizationError> {
        const COMMENT: &str = "EANs";

        log::info!("Preparing {COMMENT}");

        let mut uniqueness_check = HashSet::new();
        let mut eans = Vec::<gather::IdEntry>::new();
        let mut ean_edges = Vec::<gather::Edge>::new();
        for product in products.values() {
            for ean in &product.ids.eans {
                let ean_ki = collections::product_ean(ean);
                let product_ki = collections::product(&product.db_key);
                uniqueness_check.insert(ean_ki.key.clone());
                eans.push(gather::IdEntry { db_key: ean_ki.key });
                ean_edges.push(gather::Edge { from: ean_ki.id, to: product_ki.id });
            }
        }

        // Sanity check: all keys should be unique
        Self::uniqueness_check(&uniqueness_check, &eans, COMMENT)?;

        Ok((eans, ean_edges))
    }

    /// Prepares GTIN data.
    ///
    /// This data is needed to implement an efficient GTIN search index.
    /// Data is composed from GTIN vertex collection and edge collection connecting them to products.
    fn prepare_gtins(
        products: &BTreeMap<gather::ProductId, gather::Product>,
    ) -> Result<(Vec<gather::IdEntry>, Vec<gather::Edge>), errors::CrystalizationError> {
        const COMMENT: &str = "GTINs";

        log::info!("Preparing {COMMENT}");

        let mut uniqueness_check = HashSet::new();
        let mut gtins = Vec::<gather::IdEntry>::new();
        let mut gtin_edges = Vec::<gather::Edge>::new();
        for product in products.values() {
            for gtin in &product.ids.gtins {
                let gtin_ki = collections::product_gtin(gtin);
                let product_ki = collections::product(&product.db_key);
                uniqueness_check.insert(gtin_ki.key.clone());
                gtins.push(gather::IdEntry { db_key: gtin_ki.key });
                gtin_edges.push(gather::Edge { from: gtin_ki.id, to: product_ki.id });
            }
        }

        // Sanity check: all keys should be unique
        Self::uniqueness_check(&uniqueness_check, &gtins, COMMENT)?;

        Ok((gtins, gtin_edges))
    }

    /// Prepares Wikidata ID data.
    ///
    /// This data is needed to implement an efficient Wikidata ID search index.
    /// Data is composed from Wikidata ID vertex collection and edge collection connecting them to products.
    fn prepare_product_wiki_ids(
        products: &BTreeMap<gather::ProductId, gather::Product>,
    ) -> Result<(Vec<gather::IdEntry>, Vec<gather::Edge>), errors::CrystalizationError> {
        const COMMENT: &str = "product Wiki IDs";

        log::info!("Preparing {COMMENT}");

        let mut uniqueness_check = HashSet::new();
        let mut ids = Vec::<gather::IdEntry>::new();
        let mut id_edges = Vec::<gather::Edge>::new();
        for product in products.values() {
            for id in &product.ids.wiki {
                let wiki_ki = collections::product_wiki(id);
                let product_ki = collections::product(&product.db_key);
                uniqueness_check.insert(wiki_ki.key.clone());
                ids.push(gather::IdEntry { db_key: wiki_ki.key });
                id_edges.push(gather::Edge { from: wiki_ki.id, to: product_ki.id });
            }
        }

        // Sanity check: all keys should be unique
        Self::uniqueness_check(&uniqueness_check, &ids, COMMENT)?;

        Ok((ids, id_edges))
    }

    /// Prepares category data.
    ///
    /// This data is needed to implement an efficient alternative product search index.
    /// Data is composed from category vertex collection and edge collection connecting them to products.
    fn prepare_categories(
        products: &BTreeMap<gather::ProductId, gather::Product>,
    ) -> Result<(Vec<gather::IdEntry>, Vec<gather::Edge>), errors::CrystalizationError> {
        const COMMENT: &str = "categories";

        log::info!("Preparing {COMMENT}");

        let mut category_to_products = BTreeMap::<String, BTreeSet<gather::ProductId>>::new();
        for product in products.values() {
            for category in &product.categories {
                category_to_products
                    .entry(category.clone())
                    .and_modify(|e| {
                        e.insert(product.db_key.clone());
                    })
                    .or_insert_with(|| {
                        let mut set = BTreeSet::new();
                        set.insert(product.db_key.clone());
                        set
                    });
            }
        }

        let mut uniqueness_check = HashSet::new();
        let mut categories = Vec::<gather::IdEntry>::new();
        let mut category_edges = Vec::<gather::Edge>::new();
        for (category, product_ids) in category_to_products {
            if product_ids.len() < MAX_CATEGORY_PRODUCT_NUM {
                let category_ki = collections::category(&category);
                uniqueness_check.insert(category_ki.key.clone());
                categories.push(gather::IdEntry { db_key: category_ki.key });
                for product_id in product_ids {
                    let product_ki = collections::product(&product_id);
                    category_edges
                        .push(gather::Edge { from: category_ki.id.clone(), to: product_ki.id });
                }
            } else {
                log::info!(
                    " - skipping category `{}` with {} products",
                    category,
                    product_ids.len()
                );
            }
        }

        // Sanity check: all keys should be unique
        Self::uniqueness_check(&uniqueness_check, &categories, COMMENT)?;

        Ok((categories, category_edges))
    }

    /// Prepares manufacturing data.
    ///
    /// Data is composed from edges connecting produects to their manufacturers.
    fn prepare_manufacturing(
        products: &BTreeMap<gather::ProductId, gather::Product>,
    ) -> Vec<gather::Edge> {
        log::info!("Preparing manufacturing");
        let mut manufacturing_edges = Vec::<gather::Edge>::new();
        for product in products.values() {
            let product_ki = collections::product(&product.db_key);
            for organisation_id in &product.manufacturer_ids {
                let organisation_ki = collections::organisation(organisation_id);
                manufacturing_edges
                    .push(gather::Edge { from: organisation_ki.id, to: product_ki.id.clone() });
            }
        }
        manufacturing_edges
    }

    /// Saves organisations.
    fn save_organisations(
        &self,
        mut organisations: Vec<store::Organisation>,
    ) -> Result<(), errors::ProcessingError> {
        log::info!("Saving {} organisations", organisations.len());
        organisations.sort_by(|a, b| a.ids.cmp(&b.ids));
        serde_jsonlines::write_json_lines(&self.config.organisations_path, &organisations)?;
        Ok(())
    }

    /// Saves organisation keywords.
    fn save_organisation_keywords(
        &self,
        organisation_keywords: (Vec<gather::Keyword>, Vec<gather::Edge>),
    ) -> Result<(), errors::ProcessingError> {
        let (mut organisation_keywords, mut organisation_keyword_edges) = organisation_keywords;

        log::info!("Saving {} organisation keywords", organisation_keywords.len());
        organisation_keywords.sort();
        serde_jsonlines::write_json_lines(
            &self.config.organisation_keywords_path,
            &organisation_keywords,
        )?;

        log::info!("Saving {} organisation keyword edges", organisation_keyword_edges.len());
        organisation_keyword_edges.sort();
        serde_jsonlines::write_json_lines(
            &self.config.organisation_keyword_edges_path,
            &organisation_keyword_edges,
        )?;

        Ok(())
    }

    /// Saves VAT numbers.
    fn save_organisation_vat_ids(
        &self,
        vat_ids: (Vec<gather::IdEntry>, Vec<gather::Edge>),
    ) -> Result<(), errors::ProcessingError> {
        let (mut vat_ids, mut vat_id_edges) = vat_ids;

        log::info!("Saving {} VAT IDs", vat_ids.len());
        vat_ids.sort();
        serde_jsonlines::write_json_lines(&self.config.organisation_vat_ids_path, &vat_ids)?;

        log::info!("Saving {} VAT ID edges", vat_id_edges.len());
        vat_id_edges.sort();
        serde_jsonlines::write_json_lines(
            &self.config.organisation_vat_id_edges_path,
            &vat_id_edges,
        )?;

        Ok(())
    }

    /// Saves organisation Wikidata IDs.
    fn save_organisation_wiki_ids(
        &self,
        organisation_wiki_ids: (Vec<gather::IdEntry>, Vec<gather::Edge>),
    ) -> Result<(), errors::ProcessingError> {
        let (mut organisation_wiki_ids, mut organisation_wiki_id_edges) = organisation_wiki_ids;

        log::info!("Saving {} organisation Wiki IDs", organisation_wiki_ids.len());
        organisation_wiki_ids.sort();
        serde_jsonlines::write_json_lines(
            &self.config.organisation_wiki_ids_path,
            &organisation_wiki_ids,
        )?;

        log::info!("Saving {} organisation Wiki ID edges", organisation_wiki_id_edges.len());
        organisation_wiki_id_edges.sort();
        serde_jsonlines::write_json_lines(
            &self.config.organisation_wiki_id_edges_path,
            &organisation_wiki_id_edges,
        )?;

        Ok(())
    }

    /// Saves organisation WWW domains.
    fn save_organisation_wwws(
        &self,
        organisation_wwws: (Vec<gather::IdEntry>, Vec<gather::Edge>),
    ) -> Result<(), errors::ProcessingError> {
        let (mut organisation_wwws, mut organisation_www_edges) = organisation_wwws;

        log::info!("Saving {} organisation WWW domains", organisation_wwws.len());
        organisation_wwws.sort();
        serde_jsonlines::write_json_lines(&self.config.organisation_wwws_path, &organisation_wwws)?;

        log::info!("Saving {} organisation WWW domain edges", organisation_www_edges.len());
        organisation_www_edges.sort();
        serde_jsonlines::write_json_lines(
            &self.config.organisation_www_edges_path,
            &organisation_www_edges,
        )?;

        Ok(())
    }

    /// Saves products.
    fn save_products(
        &self,
        mut products: Vec<store::Product>,
    ) -> Result<(), errors::ProcessingError> {
        log::info!("Saving {} products.", products.len());
        products.sort_by(|a, b| a.ids.cmp(&b.ids));
        serde_jsonlines::write_json_lines(&self.config.products_path, &products)?;
        Ok(())
    }

    /// Saves product keywords.
    fn save_product_keywords(
        &self,
        product_keywords: (Vec<gather::Keyword>, Vec<gather::Edge>),
    ) -> Result<(), errors::ProcessingError> {
        let (mut product_keywords, mut product_keyword_edges) = product_keywords;

        log::info!("Saving {} product keywords", product_keywords.len());
        product_keywords.sort();
        serde_jsonlines::write_json_lines(&self.config.product_keywords_path, &product_keywords)?;

        log::info!("Saving {} product keyword edges", product_keyword_edges.len());
        product_keyword_edges.sort();
        serde_jsonlines::write_json_lines(
            &self.config.product_keyword_edges_path,
            &product_keyword_edges,
        )?;

        Ok(())
    }

    /// Saves EANs.
    fn save_eans(
        &self,
        eans: (Vec<gather::IdEntry>, Vec<gather::Edge>),
    ) -> Result<(), errors::ProcessingError> {
        let (mut eans, mut ean_edges) = eans;

        log::info!("Saving {} product EANs", eans.len());
        eans.sort();
        serde_jsonlines::write_json_lines(&self.config.product_eans_path, &eans)?;

        log::info!("Saving {} product EAN edges", ean_edges.len());
        ean_edges.sort();
        serde_jsonlines::write_json_lines(&self.config.product_ean_edges_path, &ean_edges)?;

        Ok(())
    }

    /// Saves GTINs.
    fn save_gtins(
        &self,
        gtins: (Vec<gather::IdEntry>, Vec<gather::Edge>),
    ) -> Result<(), errors::ProcessingError> {
        let (mut gtins, mut gtin_edges) = gtins;

        log::info!("Saving {} product GTINs", gtins.len());
        gtins.sort();
        serde_jsonlines::write_json_lines(&self.config.product_gtins_path, &gtins)?;

        log::info!("Saving {} product GTIN edges", gtin_edges.len());
        gtin_edges.sort();
        serde_jsonlines::write_json_lines(&self.config.product_gtin_edges_path, &gtin_edges)?;

        Ok(())
    }

    /// Saves product Wikidata IDs.
    fn save_product_wiki_ids(
        &self,
        product_wiki_ids: (Vec<gather::IdEntry>, Vec<gather::Edge>),
    ) -> Result<(), errors::ProcessingError> {
        let (mut product_wiki_ids, mut product_wiki_id_edges) = product_wiki_ids;

        log::info!("Saving {} product Wiki IDs", product_wiki_ids.len());
        product_wiki_ids.sort();
        serde_jsonlines::write_json_lines(&self.config.product_wiki_ids_path, &product_wiki_ids)?;

        log::info!("Saving {} product Wiki ID edges", product_wiki_id_edges.len());
        product_wiki_id_edges.sort();
        serde_jsonlines::write_json_lines(
            &self.config.product_wiki_id_edges_path,
            &product_wiki_id_edges,
        )?;

        Ok(())
    }

    /// Saves categories.
    fn save_categories(
        &self,
        categories: (Vec<gather::IdEntry>, Vec<gather::Edge>),
    ) -> Result<(), errors::ProcessingError> {
        let (mut categories, mut category_edges) = categories;

        log::info!("Saving {} product categories", categories.len());
        categories.sort();
        serde_jsonlines::write_json_lines(&self.config.categories_path, &categories)?;

        log::info!("Saving {} product category edges", category_edges.len());
        category_edges.sort();
        serde_jsonlines::write_json_lines(&self.config.category_edges_path, &category_edges)?;

        Ok(())
    }

    /// Saves product to organisation edges.
    fn save_manufacturing(
        &self,
        mut manufacturing_edges: Vec<gather::Edge>,
    ) -> Result<(), errors::ProcessingError> {
        log::info!("Saving {} manufacturing edges", manufacturing_edges.len());
        manufacturing_edges.sort();
        serde_jsonlines::write_json_lines(
            &self.config.manufacturing_edges_path,
            &manufacturing_edges,
        )?;
        Ok(())
    }

    fn save_all(
        self,
        mut collector: CrystalizationCollector,
    ) -> Result<(), errors::ProcessingError> {
        log::info!("Saving");

        Self::finalize(&collector.organisations, &mut collector.products);

        {
            let manufacturing_edges = Self::prepare_manufacturing(&collector.products);
            self.save_manufacturing(manufacturing_edges)?;
        }
        {
            let organisation_keywords =
                Self::prepare_organisation_keywords(&collector.organisations)?;
            self.save_organisation_keywords(organisation_keywords)?;
        }
        {
            let organisation_vat_ids =
                Self::prepare_organisation_vat_ids(&collector.organisations)?;
            self.save_organisation_vat_ids(organisation_vat_ids)?;
        }
        {
            let organisation_wiki_ids =
                Self::prepare_organisation_wiki_ids(&collector.organisations)?;
            self.save_organisation_wiki_ids(organisation_wiki_ids)?;
        }
        {
            let organisation_wwws = Self::prepare_organisation_wwws(&collector.organisations)?;
            self.save_organisation_wwws(organisation_wwws)?;
        }
        {
            let organisations = Self::prepare_organisations(collector.organisations);
            self.save_organisations(organisations)?;
        }
        {
            let product_keywords = Self::prepare_product_keywords(&collector.products)?;
            self.save_product_keywords(product_keywords)?;
        }
        {
            let eans = Self::prepare_eans(&collector.products)?;
            self.save_eans(eans)?;
        }
        {
            let gtins = Self::prepare_gtins(&collector.products)?;
            self.save_gtins(gtins)?;
        }
        {
            let product_wiki_ids = Self::prepare_product_wiki_ids(&collector.products)?;
            self.save_product_wiki_ids(product_wiki_ids)?;
        }
        {
            let categories = Self::prepare_categories(&collector.products)?;
            self.save_categories(categories)?;
        }
        {
            let products = Self::prepare_products(collector.products);
            self.save_products(products)?;
        }

        log::info!("Condensation finished");

        Ok(())
    }
}

pub struct Crystalizer;

impl Crystalizer {
    pub fn run(config: &config::CrystalizationConfig) -> Result<(), errors::ProcessingError> {
        futures::executor::block_on(async {
            let (substrates, mut report1) = Substrates::prepare(&config.substrate.substrate_path)?;
            let (groups, report2) = Grouper::group(&substrates, config)?;
            let (collector, report3) = Processor::new().process(&substrates, &groups)?;
            report1.merge(report2);
            report1.merge(report3);
            report1.report(&substrates);
            Saver::new((*config.target).clone()).save_all(collector)?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod test {
    use serde::{Deserialize, Serialize};

    use super::{
        gather, Bucket, DataSetId, ExternalId, IdCombiner, IdStructure, IndividualId, InnerId,
        UniqueId,
    };

    fn e(data_set_id: usize, inner_id: usize) -> ExternalId {
        ExternalId::new(DataSetId(data_set_id), InnerId::new(inner_id.to_string()))
    }

    fn u(unique_id: usize) -> UniqueTestId {
        UniqueTestId(unique_id)
    }

    #[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
    struct UniqueTestId(usize);

    impl UniqueId for UniqueTestId {
        fn zero() -> Self {
            Self(0)
        }

        fn increment(&mut self) {
            self.0 += 1;
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TestIds {
        pub external: ExternalId,
        pub a: Option<u32>,
        pub b: Option<i16>,
        pub c: Option<&'static str>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
    enum IndividualTestId {
        A(u32),
        B(i16),
        C(String),
    }

    impl IndividualId for IndividualTestId {}

    impl IdStructure for TestIds {
        type IndividualId = IndividualTestId;

        fn get_external_id(&self) -> ExternalId {
            self.external.clone()
        }

        fn get_individual_ids(&self) -> Vec<Self::IndividualId> {
            let mut result = Vec::with_capacity(3);
            if let Some(a) = &self.a {
                result.push(IndividualTestId::A(*a));
            }
            if let Some(b) = &self.b {
                result.push(IndividualTestId::B(*b));
            }
            if let Some(c) = &self.c {
                result.push(IndividualTestId::C(c.to_string()));
            }
            result
        }
    }

    struct TestKvStore {
        store: kv::Store,
    }

    impl TestKvStore {
        pub fn new() -> Self {
            Self { store: kv::Store::new(kv::Config::new(tempfile::tempdir().unwrap())).unwrap() }
        }

        pub fn get_external_to_individuals_bucket<'a>(
            &'a self,
        ) -> Bucket<ExternalId, Vec<IndividualTestId>> {
            let bucket =
                self.store.bucket::<Vec<u8>, Vec<u8>>(Some("external_to_individuals")).unwrap();
            Bucket { bucket, phantom: std::marker::PhantomData }
        }

        pub fn get_individual_to_externals_bucket<'a>(
            &'a self,
        ) -> Bucket<IndividualTestId, Vec<ExternalId>> {
            let bucket =
                self.store.bucket::<Vec<u8>, Vec<u8>>(Some("individual_to_externals")).unwrap();
            Bucket { bucket, phantom: std::marker::PhantomData }
        }
    }

    #[test]
    fn organisation_id() {
        let mut id = gather::OrganisationId::zero();
        assert_eq!(id.get_value(), 0);
        id.increment();
        assert_eq!(id.get_value(), 1);
        id.increment();
        assert_eq!(id.get_value(), 2);
    }

    #[test]
    fn product_id() {
        let mut id = gather::ProductId::zero();
        assert_eq!(id.get_value(), 0);
        id.increment();
        assert_eq!(id.get_value(), 1);
        id.increment();
        assert_eq!(id.get_value(), 2);
    }

    #[test]
    fn id_combiner_distinct_ids() {
        let ids = vec![
            TestIds { external: e(1, 1), a: Some(1), b: Some(-1), c: Some("1") },
            TestIds { external: e(1, 5), a: None, b: None, c: None },
            TestIds { external: e(1, 3), a: Some(3), b: Some(-3), c: Some("3") },
            TestIds { external: e(1, 2), a: Some(2), b: Some(-2), c: Some("2") },
            TestIds { external: e(1, 4), a: None, b: None, c: None },
        ];

        let expected_external_to_unique = maplit::btreemap! {
            e(1, 1) => u(1),
            e(1, 2) => u(4),
            e(1, 3) => u(3),
            e(1, 4) => u(5),
            e(1, 5) => u(2),
        };

        let store = TestKvStore::new();
        let combiner = IdCombiner::<TestIds, IndividualTestId, UniqueTestId>::new(
            store.get_external_to_individuals_bucket(),
            store.get_individual_to_externals_bucket(),
        );
        let result = combiner.combine(&ids).unwrap();

        assert_eq!(result.external_to_unique, expected_external_to_unique);
        assert_eq!(result.num_empty_ids, 2);
    }

    #[test]
    fn id_combiner_mixed_ids() {
        let ids = vec![
            TestIds { external: e(1, 1), a: Some(10), b: Some(14), c: None }, // group 1
            TestIds { external: e(2, 1), a: Some(10), b: None, c: Some("18") }, // group 1
            TestIds { external: e(3, 1), a: Some(20), b: Some(20), c: Some("20") }, // group 2
            TestIds { external: e(4, 1), a: None, b: Some(31), c: None },     // group 3
            TestIds { external: e(5, 1), a: Some(32), b: Some(31), c: Some("33") }, // group 3
            TestIds { external: e(6, 1), a: Some(40), b: Some(40), c: None }, // group 4
            TestIds { external: e(7, 1), a: Some(11), b: Some(14), c: Some("15") }, // group 1
            TestIds { external: e(8, 1), a: Some(12), b: Some(13), c: Some("15") }, // group 1
            TestIds { external: e(9, 1), a: None, b: None, c: None },         // group 5
            TestIds { external: e(0, 1), a: None, b: Some(54), c: Some("53") }, // group 6
        ];

        let expected_external_to_unique = maplit::btreemap! {
            e(1, 1) => u(1),
            e(2, 1) => u(1),
            e(3, 1) => u(2),
            e(4, 1) => u(3),
            e(5, 1) => u(3),
            e(6, 1) => u(4),
            e(7, 1) => u(1),
            e(8, 1) => u(1),
            e(9, 1) => u(5),
            e(0, 1) => u(6),
        };

        let store = TestKvStore::new();
        let combiner = IdCombiner::<TestIds, IndividualTestId, UniqueTestId>::new(
            store.get_external_to_individuals_bucket(),
            store.get_individual_to_externals_bucket(),
        );
        let result = combiner.combine(&ids).unwrap();

        assert_eq!(result.external_to_unique, expected_external_to_unique);
        assert_eq!(result.num_empty_ids, 1);
    }
}
