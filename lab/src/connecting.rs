// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use merge::Merge;
use serde::Serialize;

use transpaer_collecting::{errors::MapSerde, eu_ecolabel, open_food_facts, transpaer};
use transpaer_wikidata::data::{Entity, Item};

use crate::{config, errors, parallel, runners, utils, wikidata::ItemExt};

/// Calculates similarity of entry in some data to entry in Wikidata.
#[derive(Serialize, Clone, Debug, Hash, PartialEq, Eq)]
struct Matcher {
    /// Name of a company.
    name: String,

    /// VAT ID number of a company,
    vat_number: Option<String>,
}

impl Matcher {
    /// Constructs a new `Matcher`.
    pub fn new(name: String, vat_number: Option<String>) -> Self {
        Self { name, vat_number }
    }

    /// Integrates more data from the given record if that record has more detailed info.
    pub fn absorb_eu_ecolabel_record(&mut self, record: &eu_ecolabel::data::Record) {
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
            .map(|l| strsim::normalized_levenshtein(&self.name, &utils::disambiguate_name(l)))
            .fold(0.0, f64::max)
    }
}

impl From<open_food_facts::data::Record> for Matcher {
    fn from(r: open_food_facts::data::Record) -> Self {
        Self::new(r.brand_owner, None)
    }
}

impl merge::Merge for Matcher {
    fn merge(&mut self, other: Self) {
        self.vat_number = self.vat_number.clone().or(other.vat_number);
    }
}

/// Helper structure aggregating gathered data.
#[derive(Serialize, Clone, Debug)]
struct Entry {
    /// Matcher.
    matcher: Matcher,

    /// IDs with the highest similarity score.
    ids: HashSet<transpaer_wikidata::data::Id>,

    /// The value of the similarity score.
    similarity: f64,
}

impl Entry {
    /// Constructs a new `Entry`.
    pub fn new(matcher: Matcher) -> Self {
        Self { matcher, ids: HashSet::default(), similarity: 0.0 }
    }

    /// Evaluates the item and updates self if the item fits the matcher better than the best item found so far.
    pub fn process(&mut self, item: &Item) {
        let similarity = self.matcher.calc_similarity(item);
        match self.similarity.partial_cmp(&similarity) {
            Some(std::cmp::Ordering::Equal) => {
                self.ids.insert(item.id);
            }
            Some(std::cmp::Ordering::Less) => {
                self.ids.clear();
                self.ids.insert(item.id);
                self.similarity = similarity;
            }
            _ => {}
        }
    }
}

impl From<&Entry> for transpaer::data::NameMatching {
    fn from(entry: &Entry) -> Self {
        Self {
            name: entry.matcher.name.clone(),
            ids: entry.ids.iter().copied().collect(),
            similarity: entry.similarity,
        }
    }
}

impl merge::Merge for Entry {
    fn merge(&mut self, other: Self) {
        match self.similarity.partial_cmp(&other.similarity) {
            Some(std::cmp::Ordering::Equal) => {
                self.ids.extend(other.ids);
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
    data: HashMap<String, Matcher>,
}

impl ConnectionSources {
    fn load(config: &config::ConnectionConfig) -> Result<Self, errors::ProcessingError> {
        let mut eu_data = HashMap::<String, Matcher>::new();
        for record in eu_ecolabel::reader::parse(&config.eu_ecolabel_input_path)? {
            let name = utils::disambiguate_name(&record.company_name);
            eu_data
                .entry(name.clone())
                .and_modify(|matcher| matcher.absorb_eu_ecolabel_record(&record))
                .or_insert_with(|| Matcher::new(name, record.prepare_vat_number()));
        }
        log::info!("Found {} companies in the EU Ecolabel dataset", eu_data.len());

        let mut off_data = HashMap::<String, Matcher>::new();
        for record in open_food_facts::reader::parse(&config.open_food_facts_input_path)? {
            let record = record?;
            if record.brand_owner.is_empty() {
                for label in record.extract_brand_labels() {
                    let name = utils::disambiguate_name(&label);
                    off_data.insert(name.clone(), Matcher::new(name, None));
                }
            } else {
                let name = utils::disambiguate_name(&record.brand_owner);
                off_data.insert(name.clone(), Matcher::new(name, None));
            }
        }
        log::info!("Found {} companies and brands in Food Facts dataset", off_data.len());

        let eu_names: HashSet<String> = eu_data.keys().cloned().collect();
        let off_names: HashSet<String> = off_data.keys().cloned().collect();
        let num_common = eu_names.intersection(&off_names).count();
        utils::merge_hashmaps(&mut eu_data, off_data);

        println!("Matching {} names ({} names in common)", eu_data.len(), num_common);

        Ok(Self { data: eu_data })
    }
}

/// Data storage for gathered data.
///
/// Allows merging different instances.
#[derive(Debug, Clone)]
pub struct ConnectionCollector {
    data: HashMap<String, Entry>,
}

impl ConnectionCollector {
    #[must_use]
    pub fn new(sources: &ConnectionSources) -> Self {
        let mut data = HashMap::new();
        for (name, matcher) in &sources.data {
            data.insert(name.clone(), Entry::new(matcher.clone()));
        }
        Self { data }
    }

    fn new_empty() -> Self {
        Self { data: HashMap::new() }
    }
}

impl merge::Merge for ConnectionCollector {
    fn merge(&mut self, other: Self) {
        utils::merge_hashmaps(&mut self.data, other.data);
    }
}

#[derive(Clone, Debug)]
pub struct ConnectionWorker {
    collector: ConnectionCollector,
}

impl ConnectionWorker {
    #[must_use]
    pub fn new(collector: ConnectionCollector) -> Self {
        Self { collector }
    }
}

#[async_trait]
impl runners::WikidataWorker for ConnectionWorker {
    type Output = ConnectionCollector;

    async fn process(
        &mut self,
        _msg: &str,
        entity: Entity,
        _tx: parallel::Sender<Self::Output>,
    ) -> Result<(), errors::ProcessingError> {
        match entity {
            Entity::Item(item) => {
                if item.is_organisation() {
                    for entry in self.collector.data.values_mut() {
                        entry.process(&item);
                    }
                }
            }
            Entity::Property(_property) => (),
        }
        Ok(())
    }

    async fn finish(
        self,
        tx: parallel::Sender<Self::Output>,
    ) -> Result<(), errors::ProcessingError> {
        tx.send(self.collector).await;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct ConnectionStash {
    /// Collected data.
    collector: ConnectionCollector,

    /// Configuration.
    config: config::ConnectionConfig,
}

impl ConnectionStash {
    #[must_use]
    pub fn new(config: config::ConnectionConfig) -> Self {
        Self { collector: ConnectionCollector::new_empty(), config }
    }
}

#[async_trait]
impl runners::Stash for ConnectionStash {
    type Input = ConnectionCollector;

    fn stash(&mut self, input: Self::Input) -> Result<(), errors::ProcessingError> {
        self.collector.merge(input);
        Ok(())
    }

    fn finish(self) -> Result<(), errors::ProcessingError> {
        log::info!("Saving name matches");

        let data: Vec<transpaer::data::NameMatching> =
            self.collector.data.values().map(Into::into).collect();
        let matched = data.iter().fold(0, |acc, e| acc + usize::from(e.matched().is_some()));
        log::info!(" - matched {} / {} names", matched, self.collector.data.len());

        let contents = serde_yaml::to_string(&data).map_serde()?;
        std::fs::write(&self.config.output_path, contents)
            .map_err(|e| errors::ProcessingError::Io(e, self.config.output_path.clone()))?;

        Ok(())
    }
}

pub struct ConnectionRunner;

impl ConnectionRunner {
    pub fn run(config: &config::ConnectionConfig) -> Result<(), errors::ProcessingError> {
        let sources = ConnectionSources::load(config)?;
        let collector = ConnectionCollector::new(&sources);

        let worker = ConnectionWorker::new(collector);
        let stash = ConnectionStash::new(config.clone());

        let flow = parallel::Flow::new();
        runners::WikidataRunner::flow(flow, config, worker, stash)?.join();

        Ok(())
    }
}
