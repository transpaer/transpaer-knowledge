use std::collections::HashSet;

use async_trait::async_trait;
use merge::Merge;

use sustainity_collecting::{data::WikiId, errors::MapSerde};
use sustainity_wikidata::data::Entity;

use crate::{cache, config, errors, parallel, runners, sources::Sourceable, wikidata::ItemExt};

/// Holds all the supplementary source data.
#[derive(Debug)]
pub struct PrefilteringSources;

impl Sourceable for PrefilteringSources {
    type Config = config::PrefilteringConfig;

    fn load(_config: &Self::Config) -> Result<Self, errors::ProcessingError> {
        Ok(Self)
    }
}

/// Data storage for gathered data.
///
/// Allows merging different instances.
#[derive(Default, Debug, Clone)]
pub struct PrefilteringCollector {
    /// IDs of manufacturers.
    manufacturer_ids: HashSet<WikiId>,

    /// IDs of product classes.
    classes: HashSet<WikiId>,
}

impl PrefilteringCollector {
    pub fn add_manufacturer_ids(&mut self, ids: &[WikiId]) {
        self.manufacturer_ids.extend(ids.iter().cloned());
    }

    pub fn add_classes(&mut self, classes: &[WikiId]) {
        self.classes.extend(classes.iter().cloned());
    }
}

impl merge::Merge for PrefilteringCollector {
    fn merge(&mut self, other: Self) {
        self.manufacturer_ids.extend(other.manufacturer_ids);
        self.classes.extend(other.classes);
    }
}

/// Filters product entries out from the wikidata dump file.
#[derive(Clone, Debug, Default)]
pub struct PrefilteringWorker {
    collector: PrefilteringCollector,
}

impl PrefilteringWorker {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl runners::WikidataWorker for PrefilteringWorker {
    type Output = PrefilteringCollector;

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
pub struct PrefilteringStash {
    /// Collected data.
    collector: PrefilteringCollector,

    /// Configuration.
    config: config::PrefilteringConfig,
}

impl PrefilteringStash {
    #[must_use]
    pub fn new(config: config::PrefilteringConfig) -> Self {
        Self { collector: PrefilteringCollector::default(), config }
    }
}

#[async_trait]
impl runners::Stash for PrefilteringStash {
    type Input = PrefilteringCollector;

    fn stash(&mut self, input: Self::Input) -> Result<(), errors::ProcessingError> {
        self.collector.merge(input);
        Ok(())
    }

    fn finish(self) -> Result<(), errors::ProcessingError> {
        log::info!("Found {} manufacturers", self.collector.manufacturer_ids.len());
        log::info!("Found {} products or classes", self.collector.classes.len());

        let mut cache = cache::Wikidata {
            manufacturer_ids: self.collector.manufacturer_ids.iter().cloned().collect(),
            classes: self.collector.classes.iter().cloned().collect(),
        };

        cache.manufacturer_ids.sort();
        cache.classes.sort();

        let contents = serde_json::to_string_pretty(&cache).map_serde()?;
        std::fs::write(&self.config.wikidata_cache_path, contents)?;

        Ok(())
    }
}

pub struct PrefilteringRunner;

impl PrefilteringRunner {
    pub fn run(config: &config::PrefilteringConfig) -> Result<(), errors::ProcessingError> {
        let worker = PrefilteringWorker::new();
        let stash = PrefilteringStash::new(config.clone());

        let flow = parallel::Flow::new();
        runners::WikidataRunner::flow(flow, config, worker, stash)?.join();

        Ok(())
    }
}
