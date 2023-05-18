use std::io::Write;

use async_trait::async_trait;

use consumers_wikidata::data::Entity;

use crate::{
    cache, config, errors,
    processing::{Collectable, Essential, Processor, Sourceable},
};

/// Provides the core data for the processor.
#[derive(Debug)]
pub struct ManufacturerEssentials {
    /// Wikidata dump file loader.
    wiki: consumers_wikidata::dump::Loader,
}

#[async_trait]
impl Essential for ManufacturerEssentials {
    type Config = config::ManufacturerFilterConfig;

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
pub struct ManufacturerSources {
    /// Wikidata cache.
    cache: cache::Wikidata,
}

impl Sourceable for ManufacturerSources {
    type Config = config::ManufacturerFilterConfig;

    fn load(config: &Self::Config) -> Result<Self, errors::ProcessingError> {
        let cache = cache::load(&config.wikidata_cache_path)?;

        Ok(Self { cache })
    }
}

/// Data storage for gathered data.
///
/// Allows merging different instances.
#[derive(Default, Debug)]
pub struct ManufacturerCollector {
    /// Entries in wikidata about manufacturers.
    manufacturers: Vec<String>,
}

impl ManufacturerCollector {
    pub fn add_manufacturer(&mut self, manufacturer: String) {
        self.manufacturers.push(manufacturer);
    }
}

impl merge::Merge for ManufacturerCollector {
    fn merge(&mut self, other: Self) {
        self.manufacturers.extend_from_slice(&other.manufacturers);
    }
}

impl Collectable for ManufacturerCollector {}

/// Filters manufacturer entries out from the wikidata dump file.
#[derive(Debug)]
pub struct ManufacturerProcessor;

impl Processor for ManufacturerProcessor {
    type Config = config::ManufacturerFilterConfig;
    type Essentials = ManufacturerEssentials;
    type Sources = ManufacturerSources;
    type Collector = ManufacturerCollector;

    /// Handles one Wikidata entity.
    fn handle_entity(
        msg: &str,
        entity: &Entity,
        sources: &Self::Sources,
        collector: &mut Self::Collector,
    ) {
        match entity {
            Entity::Item(item) => {
                if sources.cache.has_manufacturer_id(&item.id) {
                    collector.add_manufacturer(msg.to_string());
                }
            }
            Entity::Property(_property) => (),
        }
    }

    /// Saves the result into files.
    fn save(
        config: &Self::Config,
        collector: &Self::Collector,
    ) -> Result<(), errors::ProcessingError> {
        log::info!("Found {} manufacturers", collector.manufacturers.len());
        let mut file = std::fs::File::create(&config.wikidata_manufacturers_path)?;
        for line in &collector.manufacturers {
            file.write_all(line.as_bytes())?;
            file.write_all(b"\n")?;
        }

        Ok(())
    }
}
