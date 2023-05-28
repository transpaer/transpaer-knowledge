use std::collections::HashSet;

use async_trait::async_trait;

use consumers_wikidata::data::Entity;

use crate::{
    cache, config, errors, knowledge,
    processing::{Collectable, Essential, Processor, Sourceable},
    wikidata::ItemExt,
};

/// Provides the core data for the processor.
#[derive(Debug)]
pub struct PrefilteringEssentials {
    /// Wikidata dump file loader.
    wiki: consumers_wikidata::dump::Loader,
}

#[async_trait]
impl Essential for PrefilteringEssentials {
    type Config = config::PrefilteringConfig;

    fn load(config: &Self::Config) -> Result<Self, errors::ProcessingError> {
        Ok(Self { wiki: consumers_wikidata::dump::Loader::load(&config.wikidata_dump_path)? })
    }

    async fn run(
        &mut self,
        tx: async_channel::Sender<String>,
    ) -> Result<usize, errors::ProcessingError> {
        Ok(self.wiki.run_with_channel(tx).await?)
    }
}

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
#[derive(Default, Debug)]
pub struct PrefilteringCollector {
    /// IDs of manufacturers.
    manufacturer_ids: HashSet<knowledge::Id>,
}

impl PrefilteringCollector {
    pub fn add_manufacturer_ids(&mut self, ids: &[knowledge::Id]) {
        for id in ids {
            self.manufacturer_ids.insert(id.clone());
        }
    }
}

impl merge::Merge for PrefilteringCollector {
    fn merge(&mut self, other: Self) {
        self.manufacturer_ids.extend(other.manufacturer_ids);
    }
}

impl Collectable for PrefilteringCollector {}

/// Filters product entries out from the wikidata dump file.
#[derive(Debug)]
pub struct PrefilteringProcessor;

impl Processor for PrefilteringProcessor {
    type Config = config::PrefilteringConfig;
    type Essentials = PrefilteringEssentials;
    type Sources = PrefilteringSources;
    type Collector = PrefilteringCollector;

    /// Handles one Wikidata entity.
    fn handle_entity(
        _msg: &str,
        entity: &Entity,
        _sources: &Self::Sources,
        collector: &mut Self::Collector,
        _config: &Self::Config,
    ) -> Result<(), errors::ProcessingError> {
        match entity {
            Entity::Item(item) => {
                if let Some(manufacturer_ids) = item.get_manufacturer_ids() {
                    collector.add_manufacturer_ids(&manufacturer_ids);
                }
            }
            Entity::Property(_property) => (),
        }
        Ok(())
    }

    /// Saves the result into files.
    fn finalize(
        collector: &Self::Collector,
        config: &Self::Config,
    ) -> Result<(), errors::ProcessingError> {
        log::info!("Found {} manufacturers", collector.manufacturer_ids.len());

        let cache = cache::Wikidata {
            manufacturer_ids: collector.manufacturer_ids.iter().cloned().collect(),
        };

        let contents = serde_json::to_string_pretty(&cache)?;
        std::fs::write(&config.wikidata_cache_path, contents)?;

        Ok(())
    }
}
