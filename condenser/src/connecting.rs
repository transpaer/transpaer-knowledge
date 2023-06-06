use std::collections::{HashMap, HashSet};

use serde::Serialize;

use async_trait::async_trait;

use sustainity_collecting::{eu_ecolabel, sustainity};
use sustainity_wikidata::data::{Entity, Item};

use crate::{
    config, errors, knowledge,
    processing::{Collectable, Essential, Processor, Sourceable},
    wikidata::ItemExt,
};

/// Provides the core data for the processor.
#[derive(Debug)]
pub struct ConnectionEssentials {
    /// Wikidata dump file loader.
    wiki: sustainity_wikidata::dump::Loader,
}

#[async_trait]
impl Essential for ConnectionEssentials {
    type Config = config::ConnectionConfig;

    fn load(config: &Self::Config) -> Result<Self, errors::ProcessingError> {
        Ok(Self { wiki: sustainity_wikidata::dump::Loader::load(&config.wikidata_path)? })
    }

    async fn run(
        &mut self,
        tx: async_channel::Sender<String>,
    ) -> Result<usize, errors::ProcessingError> {
        Ok(self.wiki.run_with_channel(tx).await?)
    }
}

/// Calculates similarity of entry in some data to entry in Wikidata.
#[derive(Serialize, Clone, Debug, Hash, PartialEq, Eq)]
struct Matcher {
    /// Name of a company.
    name: String,

    /// VAT ID number of a company,
    vat_number: Option<String>,
}

impl Matcher {
    /// Integrates more data from the given record if that record has more detailed info.
    pub fn absorb(&mut self, record: &eu_ecolabel::data::Record) {
        if record.vat_number.is_some() && self.vat_number.is_none() {
            self.vat_number = record.prepare_vat_number();
        }
    }

    /// Calculates similarity with an item.
    pub fn calc_similarity(&self, item: &Item) -> f64 {
        if let Some(item_vat_numbers) = item.get_eu_vat_numbers() {
            if let Some(self_vat_number) = &self.vat_number {
                if item_vat_numbers.contains(self_vat_number) {
                    return 10.0;
                }
            }
        }

        item.get_all_labels_and_aliases()
            .iter()
            .map(|l| strsim::normalized_levenshtein(&self.name, &Self::prepare_name(l)))
            .fold(0.0, f64::max)
    }

    /// Prepares name for easy comparison.
    fn prepare_name(name: &str) -> String {
        name.trim().to_lowercase()
    }
}

impl From<eu_ecolabel::data::Record> for Matcher {
    fn from(r: eu_ecolabel::data::Record) -> Self {
        Self { name: Self::prepare_name(&r.company_name), vat_number: r.prepare_vat_number() }
    }
}

/// Helper structure aggregating gathered data.
#[derive(Serialize, Clone, Debug)]
struct Entry {
    /// Matcher.
    matcher: Matcher,

    /// Original company name.
    name: String,

    /// IDs with the highest similarity score.
    ids: HashSet<knowledge::WikiStrId>,

    /// The value of the similarity score.
    similarity: f64,
}

impl Entry {
    /// Constructs a new `Entry`.
    pub fn new(name: String, matcher: Matcher) -> Self {
        Self { matcher, name, ids: HashSet::default(), similarity: 0.0 }
    }

    /// Evaluates the item and updates self if the item fits the matcher better than the best item found so far.
    pub fn process(&mut self, item: &Item) {
        let similarity = self.matcher.calc_similarity(item);
        match self.similarity.partial_cmp(&similarity) {
            Some(std::cmp::Ordering::Equal) => {
                self.ids.insert(item.id.clone());
            }
            Some(std::cmp::Ordering::Less) => {
                self.ids.clear();
                self.ids.insert(item.id.clone());
                self.similarity = similarity;
            }
            _ => {}
        }
    }
}

impl From<eu_ecolabel::data::Record> for Entry {
    fn from(record: eu_ecolabel::data::Record) -> Self {
        Self::new(record.company_name.clone(), record.into())
    }
}

impl From<&Entry> for sustainity::data::NameMatching {
    fn from(entry: &Entry) -> Self {
        Self {
            name: entry.name.clone(),
            ids: entry.ids.iter().cloned().collect(),
            similarity: entry.similarity,
        }
    }
}

impl merge::Merge for Entry {
    fn merge(&mut self, other: Self) {
        match self.similarity.partial_cmp(&other.similarity) {
            Some(std::cmp::Ordering::Equal) => {
                self.ids.extend(other.ids.into_iter());
            }
            Some(std::cmp::Ordering::Less) => {
                self.ids = other.ids;
                self.similarity = other.similarity;
            }
            _ => {}
        }
    }
}

/// Holds all the supplementary source data.
#[derive(Debug)]
pub struct ConnectionSources {
    /// Company data.
    data: Vec<Entry>,
}

impl Sourceable for ConnectionSources {
    type Config = config::ConnectionConfig;

    fn load(config: &Self::Config) -> Result<Self, errors::ProcessingError> {
        let mut categories = HashSet::<String>::new();
        let mut data = HashMap::<String, Entry>::new();
        for record in eu_ecolabel::reader::parse(&config.input_path)? {
            if record.product_or_service == eu_ecolabel::data::ProductOrService::Product {
                categories.insert(record.group_name.clone());
            }
            data.entry(record.company_name.clone())
                .and_modify(|e| e.matcher.absorb(&record))
                .or_insert_with(|| record.clone().into());
        }
        log::info!("Found {} companies", data.len());
        Ok(Self { data: data.values().cloned().collect() })
    }
}

/// Data storage for gathered data.
///
/// Allows merging different instances.
#[derive(Default, Debug)]
pub struct ConnectionCollector {
    /// Company data.
    data: HashMap<String, Entry>,
}

impl merge::Merge for ConnectionCollector {
    fn merge(&mut self, mut other: Self) {
        for (key, entry) in &mut self.data {
            if let Some(e) = other.data.remove(key) {
                entry.merge(e);
            }
        }
        for (key, entry) in &other.data {
            if !self.data.contains_key(key) {
                self.data.insert(key.clone(), entry.clone());
            }
        }
    }
}

impl Collectable for ConnectionCollector {}

/// Filters product entries out from the wikidata dump file.
#[derive(Clone, Debug)]
pub struct ConnectionProcessor;

impl ConnectionProcessor {
    /// Constructs a new `ConnectionProcessor`.
    pub fn new() -> Self {
        Self
    }
}

impl Processor for ConnectionProcessor {
    type Config = config::ConnectionConfig;
    type Essentials = ConnectionEssentials;
    type Sources = ConnectionSources;
    type Collector = ConnectionCollector;

    fn initialize(
        &self,
        sources: &Self::Sources,
        collector: &mut Self::Collector,
        _config: &Self::Config,
    ) -> Result<(), errors::ProcessingError> {
        for e in &sources.data {
            collector.data.insert(e.name.clone(), e.clone());
        }
        Ok(())
    }

    /// Handles one Wikidata entity.
    fn handle_entity(
        &self,
        _msg: &str,
        entity: &Entity,
        _sources: &Self::Sources,
        collector: &mut Self::Collector,
        _config: &Self::Config,
    ) -> Result<(), errors::ProcessingError> {
        match entity {
            Entity::Item(item) => {
                if item.is_organisation() {
                    for entry in collector.data.values_mut() {
                        entry.process(item);
                    }
                }
            }
            Entity::Property(_property) => (),
        }
        Ok(())
    }

    /// Saves the result into files.
    fn finalize(
        &self,
        collector: &Self::Collector,
        _sources: &Self::Sources,
        config: &Self::Config,
    ) -> Result<(), errors::ProcessingError> {
        let data: Vec<sustainity::data::NameMatching> =
            collector.data.values().map(Into::into).collect();
        let matched = data.iter().fold(0, |acc, e| acc + usize::from(e.matched().is_some()));
        log::info!("Matched {} / {} companies", matched, collector.data.len());

        let contents = serde_yaml::to_string(&data)?;
        std::fs::write(&config.output_path, contents)?;

        Ok(())
    }
}
