use std::collections::HashSet;

use async_trait::async_trait;
use merge::Merge;

use transpaer_collecting::{data::WikiId, errors::MapSerde};
use transpaer_wikidata::data::Entity;

use crate::{cache, config, errors, parallel, runners, utils, wikidata::ItemExt};

/// Data storage for gathered data.
///
/// Allows merging different instances.
#[derive(Default, Debug, Clone)]
pub struct ExtractingCollector {
    /// IDs of manufacturers.
    manufacturer_ids: HashSet<WikiId>,
}

impl ExtractingCollector {
    pub fn add_manufacturer_ids(&mut self, ids: &[WikiId]) {
        self.manufacturer_ids.extend(ids.iter().copied());
    }
}

impl merge::Merge for ExtractingCollector {
    fn merge(&mut self, other: Self) {
        self.manufacturer_ids.extend(other.manufacturer_ids);
    }
}

/// Filters product entries out from the wikidata dump file.
#[derive(Clone, Debug, Default)]
pub struct ExtractingWorker {
    collector: ExtractingCollector,
}

impl ExtractingWorker {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl runners::WikidataWorker for ExtractingWorker {
    type Output = ExtractingCollector;

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
pub struct ExtractingStash {
    /// Collected data.
    collector: ExtractingCollector,

    /// Configuration.
    config: config::ExtractingConfig,
}

impl ExtractingStash {
    #[must_use]
    pub fn new(config: config::ExtractingConfig) -> Self {
        Self { collector: ExtractingCollector::default(), config }
    }
}

#[async_trait]
impl runners::Stash for ExtractingStash {
    type Input = ExtractingCollector;

    fn stash(&mut self, input: Self::Input) -> Result<(), errors::ProcessingError> {
        log::info!("Merging {} manufacturers", input.manufacturer_ids.len(),);
        self.collector.merge(input);
        Ok(())
    }

    fn finish(self) -> Result<(), errors::ProcessingError> {
        log::info!("Found {} manufacturers", self.collector.manufacturer_ids.len());

        let mut cache = cache::Wikidata {
            manufacturer_ids: self.collector.manufacturer_ids.iter().copied().collect(),
        };

        cache.manufacturer_ids.sort();

        log::info!("Serializing...");
        let contents = serde_json::to_string_pretty(&cache).map_serde()?;

        let path = &self.config.cache.wikidata_cache_path;
        log::info!("Writing to `{}`", path.display());
        utils::create_parent(path)?;
        std::fs::write(path, contents)
            .map_err(|e| errors::ProcessingError::Io(e, path.to_owned()))?;

        Ok(())
    }
}

pub struct ExtractingRunner;

impl ExtractingRunner {
    pub fn run(config: &config::ExtractingConfig) -> Result<(), errors::ProcessingError> {
        let worker = ExtractingWorker::new();
        let stash = ExtractingStash::new(config.clone());

        let flow = parallel::Flow::new();
        runners::WikidataRunner::flow(flow, config, worker, stash)?.join();

        Ok(())
    }
}
