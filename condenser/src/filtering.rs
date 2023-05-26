use std::io::Write;

use async_trait::async_trait;

use consumers_wikidata::data::{Entity, Item};

use crate::{
    cache, config, errors,
    processing::{Collectable, Essential, Processor, Sourceable},
    wikidata::ItemExt,
};

/// Provides the core data for the processor.
#[derive(Debug)]
pub struct FilteringEssentials {
    /// Wikidata dump file loader.
    wiki: consumers_wikidata::dump::Loader,
}

#[async_trait]
impl Essential for FilteringEssentials {
    type Config = config::FilteringConfig;

    fn load(config: &Self::Config) -> Result<Self, errors::ProcessingError> {
        Ok(Self { wiki: consumers_wikidata::dump::Loader::load(&config.wikidata_full_dump_path)? })
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
pub struct FilteringSources {
    /// Wikidata cache.
    cache: cache::Wikidata,
}

impl Sourceable for FilteringSources {
    type Config = config::FilteringConfig;

    fn load(config: &Self::Config) -> Result<Self, errors::ProcessingError> {
        let cache = cache::load(&config.wikidata_cache_path)?;

        Ok(Self { cache })
    }
}

/// Data storage for gathered data.
///
/// Allows merging different instances.
#[derive(Default, Debug)]
pub struct FilteringCollector {
    /// Picked entries from wikidata.
    entries: Vec<String>,
}

impl FilteringCollector {
    pub fn add_entry(&mut self, entry: String) {
        self.entries.push(entry);
    }
}

impl merge::Merge for FilteringCollector {
    fn merge(&mut self, other: Self) {
        self.entries.extend_from_slice(&other.entries);
    }
}

impl Collectable for FilteringCollector {}

/// Filters manufacturer entries out from the wikidata dump file.
#[derive(Debug)]
pub struct FilteringProcessor;

impl FilteringProcessor {
    /// Decides if the passed item should be kept or filtered out.
    ///
    /// The item is kept if it:
    /// - is present in the cache, or
    /// - has a website, or
    /// - has a manufacturer
    fn should_keep(item: &Item, sources: &FilteringSources) -> bool {
        sources.cache.has_manufacturer_id(&item.id)
            || item.has_official_website()
            || item.has_manufacturer()
    }
}

impl Processor for FilteringProcessor {
    type Config = config::FilteringConfig;
    type Essentials = FilteringEssentials;
    type Sources = FilteringSources;
    type Collector = FilteringCollector;

    /// Handles one Wikidata entity.
    fn handle_entity(
        msg: &str,
        entity: &Entity,
        sources: &Self::Sources,
        collector: &mut Self::Collector,
    ) {
        match entity {
            Entity::Item(item) => {
                if Self::should_keep(item, sources) {
                    collector.add_entry(msg.to_string());
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
        log::info!("Found {} entries", collector.entries.len());
        let mut file = std::fs::File::create(&config.wikidata_filtered_dump_path)?;
        for line in &collector.entries {
            file.write_all(line.as_bytes())?;
            file.write_all(b"\n")?;
        }

        Ok(())
    }
}
