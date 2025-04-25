use std::collections::{btree_map::Entry, BTreeMap, BTreeSet};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;

use sustainity_models::{
    buckets::{Bucket, BucketError},
    gather, ids,
};
use sustainity_schema as schema;

use crate::{
    coagulate::{Coagulate, ExternalId, InnerId, UniqueId},
    config, errors,
    substrate::{DataSetId, Substrates},
};

#[derive(Default)]
pub struct Summary {
    pub product_ids: Vec<ProductIds>,
    pub producer_ids: Vec<ProducerIds>,
}

pub struct Coagulator;

impl Coagulator {
    pub fn run(config: &config::CoagulationConfig) -> Result<(), errors::ProcessingError> {
        futures::executor::block_on(async {
            let (substrates, substrates_report) =
                Substrates::prepare(&config.substrate.substrate_path)?;
            substrates_report.report();

            let (summary, coagulator_report) = Self::summarize(&substrates)?;
            coagulator_report.report(&substrates);

            let coagulate = Self::group(&summary, config)?;
            log::info!("Saving the coagulate");
            coagulate.save(&config.coagulate, &substrates)?;

            Ok(())
        })
    }

    fn summarize(
        substrates: &Substrates,
    ) -> Result<(Summary, CoagulationReport), errors::CoagulationError> {
        log::info!("Gathering IDs");
        let mut result = Summary::default();
        let mut report = CoagulationReport::default();
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

    fn group(
        summary: &Summary,
        config: &config::CoagulationConfig,
    ) -> Result<Coagulate, errors::CoagulationError> {
        if config.runtime.exists() {
            std::fs::remove_dir_all(&config.runtime)
                .map_err(|e| errors::CoagulationError::Io(e, config.runtime.clone()))?;
        }
        let store = GroupingStore::new(&config.runtime)?;

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

            result.external_to_unique
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

            result.external_to_unique
        };

        Ok(Coagulate::new(producer, product))
    }
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
    ) -> Result<IdCombinationResult<U>, errors::CoagulationError> {
        self.fill_ids(ids)?;
        self.find_clusters(ids)
    }

    fn fill_ids(&mut self, ids: &[T]) -> Result<(), errors::CoagulationError> {
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
    ) -> Result<IdCombinationResult<U>, errors::CoagulationError> {
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
    ) -> Result<BTreeSet<ExternalId>, errors::CoagulationError> {
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
    ) -> Result<Vec<I>, errors::CoagulationError> {
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
    ) -> (Self, Vec<CoagulationWarning>) {
        let external = ExternalId::new(data_set_id, InnerId::new(producer.id.clone()));
        let (individual, warnings) = Self::extract_individual_ids(&producer.ids, data_set_id);
        (Self { external, individual }, warnings)
    }

    #[must_use]
    pub fn from_review(
        producer: &schema::ReviewProducer,
        data_set_id: DataSetId,
    ) -> (Self, Vec<CoagulationWarning>) {
        let external = ExternalId::new(data_set_id, InnerId::new(producer.id.clone()));
        let (individual, warnings) = Self::extract_individual_ids(&producer.ids, data_set_id);
        (Self { external, individual }, warnings)
    }

    #[must_use]
    fn extract_individual_ids(
        ids: &schema::ProducerIds,
        data_set_id: DataSetId,
    ) -> (Vec<IndividualProducerId>, Vec<CoagulationWarning>) {
        let mut individual = Vec::new();
        let mut warnings = Vec::new();
        if let Some(vat) = &ids.vat {
            for id in vat {
                match ids::VatId::try_from(id) {
                    Ok(id) => individual.push(IndividualProducerId::Vat(id)),
                    Err(_) => warnings.push(CoagulationWarning::InvalidIndividualId {
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
                    Err(_) => warnings.push(CoagulationWarning::InvalidIndividualId {
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
    ) -> (Self, Vec<CoagulationWarning>) {
        let external = ExternalId::new(data_set_id, InnerId::new(product.id.clone()));
        let (individual, warnings) = Self::extract_individual_ids(&product.ids, data_set_id);
        (Self { external, individual }, warnings)
    }

    #[must_use]
    pub fn from_producer(
        product: &schema::ProducerProduct,
        data_set_id: DataSetId,
    ) -> (Self, Vec<CoagulationWarning>) {
        let external = ExternalId::new(data_set_id, InnerId::new(product.id.clone()));
        let (individual, warnings) = Self::extract_individual_ids(&product.ids, data_set_id);
        (Self { external, individual }, warnings)
    }

    #[must_use]
    pub fn from_review(
        product: &schema::ReviewProduct,
        data_set_id: DataSetId,
    ) -> (Self, Vec<CoagulationWarning>) {
        let external = ExternalId::new(data_set_id, InnerId::new(product.id.clone()));
        let (individual, warnings) = Self::extract_individual_ids(&product.ids, data_set_id);
        (Self { external, individual }, warnings)
    }

    fn extract_individual_ids(
        ids: &schema::ProductIds,
        data_set_id: DataSetId,
    ) -> (Vec<IndividualProductId>, Vec<CoagulationWarning>) {
        let mut individual = Vec::new();
        let mut warnings = Vec::new();
        if let Some(ean) = &ids.ean {
            for id in ean {
                match ids::Ean::try_from(id) {
                    Ok(id) => individual.push(IndividualProductId::Ean(id)),
                    Err(_) => warnings.push(CoagulationWarning::InvalidIndividualId {
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
                    Err(_) => warnings.push(CoagulationWarning::InvalidIndividualId {
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
                    Err(_) => warnings.push(CoagulationWarning::InvalidIndividualId {
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

/// Warnings specific to the crystalisation command.
#[must_use]
#[derive(Error, Debug)]
pub enum CoagulationWarning {
    // TODO: add more detailed info about the data set and ID variant.
    #[error("Individual ID is not valid: {individual_id}, data_set: {data_set_id:?})")]
    InvalidIndividualId { data_set_id: DataSetId, individual_id: String },
}

// TODO: Rework as repotts per data source
#[must_use]
#[derive(Debug, Default)]
pub struct CoagulationReport {
    invalid_ids: BTreeMap<DataSetId, BTreeSet<String>>,
    empty_ids: BTreeMap<DataSetId, BTreeSet<InnerId>>,
    missing_inner_ids: BTreeMap<DataSetId, BTreeSet<InnerId>>,
}

impl CoagulationReport {
    pub fn add(&mut self, warning: CoagulationWarning) {
        match warning {
            CoagulationWarning::InvalidIndividualId { data_set_id, individual_id } => {
                self.add_invalid_id(data_set_id, individual_id);
            }
        }
    }

    pub fn add_many(&mut self, warnings: Vec<CoagulationWarning>) {
        for warning in warnings {
            self.add(warning);
        }
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

    pub fn report(&self, substrates: &Substrates) {
        const UNKNOWN: &str = "unknown";

        log::warn!("Coagulation report:");

        if !self.invalid_ids.is_empty() {
            log::warn!(" invalid IDs:");
            for (data_set_id, ids) in &self.invalid_ids {
                let name = substrates.get_name_for_id(*data_set_id).unwrap_or(UNKNOWN);
                log::warn!("  - {}: {}", name, ids.len());
            }
        }
        if !self.empty_ids.is_empty() {
            log::warn!(" empty IDs:");
            for (data_set_id, ids) in &self.empty_ids {
                let name = substrates.get_name_for_id(*data_set_id).unwrap_or(UNKNOWN);
                log::warn!("  - {}: {}", name, ids.len());
            }
        }
        if !self.missing_inner_ids.is_empty() {
            log::warn!(" missing inner IDs:");
            for (data_set_id, ids) in &self.missing_inner_ids {
                let name = substrates.get_name_for_id(*data_set_id).unwrap_or(UNKNOWN);
                log::warn!("  - {}: {}", name, ids.len());
            }
        }
        log::warn!("End of the report");
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
        ExternalId::new(DataSetId::new(data_set_id), InnerId::new(inner_id.to_string()))
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
