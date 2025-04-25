use std::collections::HashSet;

use async_trait::async_trait;
use merge::Merge;

use sustainity_collecting::{data::WikiId, errors::MapSerde};
use sustainity_wikidata::data::Entity;

use crate::{cache, config, errors, parallel, runners, sources::Sourceable, wikidata::ItemExt};

/// Holds all the supplementary source data.
#[derive(Debug)]
pub struct FilteringSources;

impl Sourceable for FilteringSources {
    type Config = config::Filtering1Config;

    fn load(_config: &Self::Config) -> Result<Self, errors::ProcessingError> {
        Ok(Self)
    }
}

/// Data storage for gathered data.
///
/// Allows merging different instances.
#[derive(Default, Debug, Clone)]
pub struct FilteringCollector {
    /// IDs of manufacturers.
    manufacturer_ids: HashSet<WikiId>,

    /// IDs of product classes.
    classes: HashSet<WikiId>,
}

impl FilteringCollector {
    pub fn add_manufacturer_ids(&mut self, ids: &[WikiId]) {
        self.manufacturer_ids.extend(ids.iter().copied());
    }

    pub fn add_classes(&mut self, classes: &[WikiId]) {
        self.classes.extend(classes.iter().copied());
    }
}

impl merge::Merge for FilteringCollector {
    fn merge(&mut self, other: Self) {
        self.manufacturer_ids.extend(other.manufacturer_ids);
        self.classes.extend(other.classes);
    }
}

/// Filters product entries out from the wikidata dump file.
#[derive(Clone, Debug, Default)]
pub struct FilteringWorker {
    collector: FilteringCollector,
}

impl FilteringWorker {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl runners::WikidataWorker for FilteringWorker {
    type Output = FilteringCollector;

    async fn process(
        &mut self,
        _msg: &str,
        entity: Entity,
        _tx: parallel::Sender<Self::Output>,
    ) -> Result<(), errors::ProcessingError> {
        match entity {
            Entity::Item(item) => {
                if let Some(manufacturer_ids) = item.get_manufacturer_ids()? {
                    self.collector.add_manufacturer_ids(&manufacturer_ids);
                }
                if let Some(class_ids) = item.get_superclasses()? {
                    self.collector.add_classes(&class_ids);
                }
                if let Some(class_ids) = item.get_classes()? {
                    self.collector.add_classes(&class_ids);
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
pub struct FilteringStash {
    /// Collected data.
    collector: FilteringCollector,

    /// Configuration.
    config: config::Filtering1Config,
}

impl FilteringStash {
    #[must_use]
    pub fn new(config: config::Filtering1Config) -> Self {
        Self { collector: FilteringCollector::default(), config }
    }
}

#[async_trait]
impl runners::Stash for FilteringStash {
    type Input = FilteringCollector;

    fn stash(&mut self, input: Self::Input) -> Result<(), errors::ProcessingError> {
        log::info!(
            "Merging {} manufacturers and {} products or classes",
            input.manufacturer_ids.len(),
            input.classes.len()
        );
        self.collector.merge(input);
        Ok(())
    }

    fn finish(self) -> Result<(), errors::ProcessingError> {
        log::info!("Found {} manufacturers", self.collector.manufacturer_ids.len());
        log::info!("Found {} products or classes", self.collector.classes.len());

        let mut cache = cache::Wikidata {
            manufacturer_ids: self.collector.manufacturer_ids.iter().copied().collect(),
            classes: self.collector.classes.iter().copied().collect(),
        };

        cache.manufacturer_ids.sort();
        cache.classes.sort();

        log::info!("Serializing...");
        let contents = serde_json::to_string_pretty(&cache).map_serde()?;

        log::info!("Writing to {:?}", self.config.wikidata_cache_path);
        std::fs::write(&self.config.wikidata_cache_path, contents)
            .map_err(|e| errors::ProcessingError::Io(e, self.config.wikidata_cache_path.clone()))?;

        Ok(())
    }
}

pub struct FilteringRunner;

impl FilteringRunner {
    pub fn run(config: &config::Filtering1Config) -> Result<(), errors::ProcessingError> {
        let worker = FilteringWorker::new();
        let stash = FilteringStash::new(config.clone());

        let flow = parallel::Flow::new();
        runners::WikidataRunner::flow(flow, config, worker, stash)?.join();

        Ok(())
    }
}
