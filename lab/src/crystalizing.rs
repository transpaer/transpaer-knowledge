use std::collections::{btree_map::Entry, BTreeMap, BTreeSet, HashSet};

use merge::Merge;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;

use sustainity_models::{
    buckets::{Bucket, BucketError, DbStore},
    gather, ids, store,
};
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
        schema::Regions::List(list) => gather::Regions::List(convert_region_list(list)?),
    })
}

fn convert_region_list(
    list: &schema::RegionList,
) -> Result<Vec<isocountry::CountryCode>, isocountry::CountryCodeParseErr> {
    let mut regions = Vec::new();
    for region in &list.0 {
        regions.push(isocountry::CountryCode::for_alpha3(region)?);
    }
    Ok(regions)
}

fn extract_product_origins(
    origins: Option<&schema::ProductOrigins>,
) -> Result<BTreeSet<isocountry::CountryCode>, isocountry::CountryCodeParseErr> {
    if let Some(origins) = origins {
        if let Some(regions) = &origins.regions {
            Ok(convert_region_list(regions)?.into_iter().collect())
        } else {
            Ok(BTreeSet::new())
        }
    } else {
        Ok(BTreeSet::new())
    }
}

fn extract_producer_origins(
    origins: Option<&schema::ProducerOrigins>,
) -> Result<BTreeSet<isocountry::CountryCode>, isocountry::CountryCodeParseErr> {
    if let Some(origins) = origins {
        if let Some(regions) = &origins.regions {
            Ok(convert_region_list(regions)?.into_iter().collect())
        } else {
            Ok(BTreeSet::new())
        }
    } else {
        Ok(BTreeSet::new())
    }
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
                log::warn!("  - {path:?}");
            }
        }
        if !self.not_unicode.is_empty() {
            log::warn!(" not unicode:");
            for path in &self.not_unicode {
                log::warn!("  - {path:?}");
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

trait UniqueId: Clone + Eq + Ord + std::hash::Hash {
    fn zero() -> Self;
    fn increment(&mut self);
}

impl UniqueId for gather::OrganisationId {
    fn zero() -> Self {
        Self::from_value(0)
    }

    fn increment(&mut self) {
        *self = Self::from_value(self.as_value() + 1);
    }
}

impl UniqueId for gather::ProductId {
    fn zero() -> Self {
        Self::from_value(0)
    }

    fn increment(&mut self) {
        *self = Self::from_value(self.as_value() + 1);
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

struct GroupingStore {
    store: kv::Store,
}

impl GroupingStore {
    pub fn new(path: &std::path::Path) -> Result<Self, BucketError> {
        Ok(Self { store: kv::Store::new(kv::Config::new(path))? })
    }

    pub fn get_producer_external_to_individuals_bucket(
        &self,
    ) -> Result<Bucket<ExternalId, Vec<IndividualProducerId>>, BucketError> {
        Bucket::obtain(&self.store, "producers_external_to_individuals")
    }

    pub fn get_producer_individual_to_externals_bucket(
        &self,
    ) -> Result<Bucket<IndividualProducerId, Vec<ExternalId>>, BucketError> {
        Bucket::obtain(&self.store, "producers_individual_to_externals")
    }

    pub fn get_product_external_to_individuals_bucket(
        &self,
    ) -> Result<Bucket<ExternalId, Vec<IndividualProductId>>, BucketError> {
        Bucket::obtain(&self.store, "products_external_to_individuals")
    }

    pub fn get_product_individual_to_externals_bucket(
        &self,
    ) -> Result<Bucket<IndividualProductId, Vec<ExternalId>>, BucketError> {
        Bucket::obtain(&self.store, "products_individual_to_externals")
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
    Ean(ids::Ean),

    /// GTIN.
    Gtin(ids::Gtin),

    /// Wikidata ID.
    Wiki(ids::WikiId),
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
                    Ok(id) => individual.push(IndividualProductId::Ean(id)),
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
                    Ok(id) => individual.push(IndividualProductId::Gtin(id)),
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
                    Ok(id) => individual.push(IndividualProductId::Wiki(id)),
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

        if config.runtime_storage.exists() {
            std::fs::remove_dir_all(&config.runtime_storage)
                .map_err(|e| errors::CrystalizationError::Io(e, config.runtime_storage.clone()))?;
        }
        let store = GroupingStore::new(&config.runtime_storage)?;

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
                origins: extract_producer_origins(producer.origins.as_ref()).map_err(|source| {
                    errors::CrystalizationError::IsoCountry {
                        source,
                        when: "processing catalogue producer",
                    }
                })?,
                certifications: gather::Certifications::default(),
                products: BTreeSet::new(), //< filled later
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
            self.extract_related_products(product.related.as_ref(), groups, substrate);
        let manufacturers =
            self.extract_manufacturer_ids(product.origins.as_ref(), groups, substrate);

        self.collector.update_product(
            unique_id,
            gather::Product {
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
                regions: extract_regions(product.availability.as_ref()).map_err(|source| {
                    errors::CrystalizationError::IsoCountry {
                        source,
                        when: "processing catalogue product regions",
                    }
                })?,
                origins: extract_product_origins(product.origins.as_ref()).map_err(|source| {
                    errors::CrystalizationError::IsoCountry {
                        source,
                        when: "processing catalogue product origins",
                    }
                })?,
                manufacturers,
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
            self.extract_related_products(product.related.as_ref(), groups, substrate);
        let manufacturers =
            self.extract_manufacturer_ids(product.origins.as_ref(), groups, substrate);

        self.collector.update_product(
            unique_id,
            gather::Product {
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
                regions: extract_regions(product.availability.as_ref()).map_err(|source| {
                    errors::CrystalizationError::IsoCountry {
                        source,
                        when: "processing producer product regions",
                    }
                })?,
                origins: extract_product_origins(product.origins.as_ref()).map_err(|source| {
                    errors::CrystalizationError::IsoCountry {
                        source,
                        when: "processing producer product origins",
                    }
                })?,
                manufacturers,
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
                origins: extract_producer_origins(producer.origins.as_ref()).map_err(|source| {
                    errors::CrystalizationError::IsoCountry {
                        source,
                        when: "processing review producer",
                    }
                })?,
                certifications,
                products: BTreeSet::new(), //< filled later
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
            self.extract_related_products(product.related.as_ref(), groups, substrate);
        let manufacturers =
            self.extract_manufacturer_ids(product.origins.as_ref(), groups, substrate);

        self.collector.update_product(
            unique_id.clone(),
            gather::Product {
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
                regions: extract_regions(product.availability.as_ref()).map_err(|source| {
                    errors::CrystalizationError::IsoCountry {
                        source,
                        when: "processing review product regions",
                    }
                })?,
                origins: extract_product_origins(product.origins.as_ref()).map_err(|source| {
                    errors::CrystalizationError::IsoCountry {
                        source,
                        when: "processing review product origins",
                    }
                })?,
                manufacturers,
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
        origins: Option<&schema::ProductOrigins>,
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
        related: Option<&schema::RelatedProducts>,
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

        Some(gather::BCorpCert {
            id: producer.id.clone(),
            report_url: producer
                .report
                .as_ref()
                .and_then(|report| report.url.clone())
                .unwrap_or_default(),
        })
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
    store: DbStore,
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
        organisations: &mut BTreeMap<gather::OrganisationId, gather::Organisation>,
        products: &mut BTreeMap<gather::ProductId, gather::Product>,
    ) {
        log::info!("Finalizing products");

        // Assign
        //  - certifications to products
        //  - product to organisations
        log::info!(" -> assigning certifications");
        for (product_id, product) in products.iter_mut() {
            for manufacturer_id in &product.manufacturers {
                if let Some(organisation) = organisations.get_mut(manufacturer_id) {
                    product.certifications.inherit(&organisation.certifications);
                    organisation.products.insert(product_id.clone());
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
    fn uniqueness_check<T1, T2, T3>(
        unique: &HashSet<T1>,
        all: &Bucket<T2, T3>,
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

    /// Stores organsation data.
    ///
    /// - fills left-over certifications
    /// - converts into a vector
    fn store_organisations(
        &self,
        organisations: BTreeMap<gather::OrganisationId, gather::Organisation>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "organisation.id => organisation";
        log::info!(" -> `{COMMENT}`");

        let bucket = self.store.get_organisation_bucket()?;
        for (id, organisation) in organisations {
            bucket.insert(&id, &organisation.store())?;
        }

        Ok(())
    }

    /// Stores organsation keywords data.
    ///
    /// This data is needed to implement an efficient text search index.
    fn store_organisation_keywords(
        &self,
        organisations: &BTreeMap<gather::OrganisationId, gather::Organisation>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "keywords => [organisation.id]";
        log::info!(" -> `{COMMENT}`");

        let mut data = BTreeMap::<String, Vec<store::OrganisationId>>::new();
        for (unique_id, organisation) in organisations {
            for keyword in Self::extract_keywords(&organisation.names) {
                data.entry(keyword)
                    .and_modify(|ids| ids.push(unique_id.clone()))
                    .or_insert_with(|| vec![unique_id.clone()]);
            }
        }

        let bucket = self.store.get_keyword_to_organisation_ids_bucket()?;
        for (keyword, ids) in data {
            bucket.insert(&keyword, &ids)?;
        }

        bucket.flush()?;
        Ok(())
    }

    /// Stores VAT data.
    ///
    /// This data is needed to implement an efficient VAT search index.
    fn store_organisation_vat_ids(
        &self,
        organisations: &BTreeMap<gather::OrganisationId, gather::Organisation>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "organisation.vat_id => organisation.id";

        log::info!(" -> `{COMMENT}`");

        let bucket = self.store.get_vat_id_to_organisation_id_bucket()?;

        let mut uniqueness_check = HashSet::new();
        for (organisation_id, organisation) in organisations {
            for vat_id in &organisation.ids.vat_ids {
                bucket.insert(vat_id, organisation_id)?;
                uniqueness_check.insert(vat_id);
            }
        }

        // Sanity check: all keys should be unique
        Self::uniqueness_check(&uniqueness_check, &bucket, COMMENT)?;

        bucket.flush()?;
        Ok(())
    }

    /// Stores Wikidata ID data.
    ///
    /// This data is needed to implement an efficient Wikidata ID search index.
    fn store_organisation_wiki_ids(
        &self,
        organisations: &BTreeMap<gather::OrganisationId, gather::Organisation>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "organisation.wiki_id => organisation.id";

        log::info!(" -> `{COMMENT}`");

        let bucket = self.store.get_wiki_id_to_organisation_id_bucket()?;

        let mut uniqueness_check = HashSet::new();
        for (organisation_id, organisation) in organisations {
            for wiki_id in &organisation.ids.wiki {
                bucket.insert(wiki_id, organisation_id)?;
                uniqueness_check.insert(wiki_id);
            }
        }

        // Sanity check: all keys should be unique
        Self::uniqueness_check(&uniqueness_check, &bucket, COMMENT)?;

        bucket.flush()?;
        Ok(())
    }

    /// Stores organisation WWW domain data.
    ///
    /// This data is needed to implement an efficient WWW domain search index.
    fn store_organisation_www_domains(
        &self,
        organisations: &BTreeMap<gather::OrganisationId, gather::Organisation>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "organisation.WWW_domain => organisation.id";

        log::info!(" -> `{COMMENT}`");

        let bucket = self.store.get_www_domain_to_organisation_id_bucket()?;

        let mut uniqueness_check = HashSet::new();
        for (organisation_id, organisation) in organisations {
            for domain in &organisation.ids.domains {
                bucket.insert(domain, organisation_id)?;
                uniqueness_check.insert(domain);
            }
        }

        // Sanity check: all keys should be unique
        Self::uniqueness_check(&uniqueness_check, &bucket, COMMENT)?;

        bucket.flush()?;
        Ok(())
    }

    /// Stores product data.
    fn store_products(
        &self,
        products: BTreeMap<gather::ProductId, gather::Product>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "product.id => product";
        log::info!(" -> `{COMMENT}`");

        let bucket = self.store.get_product_bucket()?;
        for (id, product) in products {
            let product = product.clone().store();
            bucket.insert(&id, &product)?;

            // Make sure that the DB can be deserialized
            assert!(bucket.get(&id).is_ok(), "DB integrity: {id:?} => {product:?}");
        }

        bucket.flush()?;
        Ok(())
    }

    /// Stores product keywords data.
    ///
    /// This data is needed to implement an efficient text search index.
    fn store_product_keywords(
        &self,
        products: &BTreeMap<gather::ProductId, gather::Product>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "keywords => [product.id]";
        log::info!(" -> `{COMMENT}`");

        let mut data = BTreeMap::<String, Vec<store::ProductId>>::new();
        for (unique_id, product) in products {
            for keyword in Self::extract_keywords(&product.names) {
                data.entry(keyword)
                    .and_modify(|ids| ids.push(unique_id.clone()))
                    .or_insert_with(|| vec![unique_id.clone()]);
            }
        }

        let bucket = self.store.get_keyword_to_product_ids_bucket()?;
        for (keyword, ids) in data {
            bucket.insert(&keyword, &ids)?;
        }

        bucket.flush()?;
        Ok(())
    }

    /// Stores EAN data.
    ///
    /// This data is needed to implement an efficient EAN search index.
    fn store_product_eans(
        &self,
        products: &BTreeMap<gather::ProductId, gather::Product>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "product.ean => product.id";

        log::info!(" -> `{COMMENT}`");

        let bucket = self.store.get_ean_to_product_id_bucket()?;

        let mut uniqueness_check = HashSet::new();
        for (product_id, product) in products {
            for ean in &product.ids.eans {
                bucket.insert(ean, product_id)?;
                uniqueness_check.insert(ean);
            }
        }

        // Sanity check: all keys should be unique
        Self::uniqueness_check(&uniqueness_check, &bucket, COMMENT)?;

        bucket.flush()?;
        Ok(())
    }

    /// Stores GTIN data.
    ///
    /// This data is needed to implement an efficient GTIN search index.
    fn store_product_gtins(
        &self,
        products: &BTreeMap<gather::ProductId, gather::Product>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "product.gtin => product.id";

        log::info!(" -> `{COMMENT}`");

        let bucket = self.store.get_gtin_to_product_id_bucket()?;

        let mut uniqueness_check = HashSet::new();
        for (product_id, product) in products {
            for gtin in &product.ids.gtins {
                bucket.insert(gtin, product_id)?;
                uniqueness_check.insert(gtin);
            }
        }

        // Sanity check: all keys should be unique
        Self::uniqueness_check(&uniqueness_check, &bucket, COMMENT)?;

        bucket.flush()?;
        Ok(())
    }

    /// Stores Wikidata ID data.
    ///
    /// This data is needed to implement an efficient Wikidata ID search index.
    /// Data is composed from Wikidata ID vertex collection and edge collection connecting them to products.
    fn store_product_wiki_ids(
        &self,
        products: &BTreeMap<gather::ProductId, gather::Product>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "product.wiki_id => product.id";

        log::info!(" -> `{COMMENT}`");

        let bucket = self.store.get_wiki_id_to_product_id_bucket()?;

        let mut uniqueness_check = HashSet::new();
        for (product_id, product) in products {
            for wiki_id in &product.ids.wiki {
                bucket.insert(wiki_id, product_id)?;
                uniqueness_check.insert(wiki_id);
            }
        }

        // Sanity check: all keys should be unique
        Self::uniqueness_check(&uniqueness_check, &bucket, COMMENT)?;

        bucket.flush()?;
        Ok(())
    }

    /// Stores category data.
    ///
    /// This data is needed to implement an efficient alternative product search index.
    /// Data is composed from category vertex collection and edge collection connecting them to products.
    fn store_categories(
        &self,
        products: &BTreeMap<gather::ProductId, gather::Product>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "product.category => [product.id]";

        log::info!(" -> `{COMMENT}`");

        let mut data = BTreeMap::<String, Vec<store::ProductId>>::new();
        for (unique_id, product) in products {
            for category in &product.categories {
                data.entry(category.clone())
                    .and_modify(|ids| ids.push(unique_id.clone()))
                    .or_insert_with(|| vec![unique_id.clone()]);
            }
        }

        let bucket = self.store.get_categories_bucket()?;
        for (keyword, ids) in data {
            if ids.len() < MAX_CATEGORY_PRODUCT_NUM {
                bucket.insert(&keyword, &ids)?;
            }
        }

        bucket.flush()?;
        Ok(())
    }

    fn store_all(
        self,
        mut collector: CrystalizationCollector,
    ) -> Result<(), errors::ProcessingError> {
        Self::finalize(&mut collector.organisations, &mut collector.products);

        log::info!("Storing:");

        self.store_organisation_keywords(&collector.organisations)?;
        self.store_organisation_vat_ids(&collector.organisations)?;
        self.store_organisation_wiki_ids(&collector.organisations)?;
        self.store_organisation_www_domains(&collector.organisations)?;
        self.store_organisations(collector.organisations)?;

        self.store_product_keywords(&collector.products)?;
        self.store_product_eans(&collector.products)?;
        self.store_product_gtins(&collector.products)?;
        self.store_product_wiki_ids(&collector.products)?;
        self.store_categories(&collector.products)?;
        self.store_products(collector.products)?;

        log::info!("Crystalisation finished");

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
            let store = DbStore::new(&config.db_storage)?;
            Saver::new(store).store_all(collector)?;
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
        ) -> Bucket<'a, ExternalId, Vec<IndividualTestId>> {
            Bucket::obtain(&self.store, "external_to_individuals").unwrap()
        }

        pub fn get_individual_to_externals_bucket<'a>(
            &'a self,
        ) -> Bucket<'a, IndividualTestId, Vec<ExternalId>> {
            Bucket::obtain(&self.store, "individual_to_externals").unwrap()
        }
    }

    #[test]
    fn organisation_id() {
        let mut id = gather::OrganisationId::zero();
        assert_eq!(id.as_value(), 0);
        id.increment();
        assert_eq!(id.as_value(), 1);
        id.increment();
        assert_eq!(id.as_value(), 2);
    }

    #[test]
    fn product_id() {
        let mut id = gather::ProductId::zero();
        assert_eq!(id.as_value(), 0);
        id.increment();
        assert_eq!(id.as_value(), 1);
        id.increment();
        assert_eq!(id.as_value(), 2);
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
