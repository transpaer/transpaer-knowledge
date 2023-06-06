use std::io::Write;

use async_trait::async_trait;

use sustainity_wikidata::data::{Entity, Item};

use crate::{
    config, errors,
    processing::{Collectable, Essential, Processor},
    sources,
};

/// Provides the core data for the processor.
#[derive(Debug)]
pub struct FilteringEssentials {
    /// Wikidata dump file loader.
    wiki: sustainity_wikidata::dump::Loader,
}

#[async_trait]
impl Essential for FilteringEssentials {
    type Config = config::FilteringConfig;

    fn load(config: &Self::Config) -> Result<Self, errors::ProcessingError> {
        Ok(Self { wiki: sustainity_wikidata::dump::Loader::load(&config.wikidata_full_dump_path)? })
    }

    async fn run(
        &mut self,
        tx: async_channel::Sender<String>,
    ) -> Result<usize, errors::ProcessingError> {
        Ok(self.wiki.run_with_channel(tx).await?)
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

    pub fn is_full(&self) -> bool {
        self.entries.len() >= 100_000
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

impl merge::Merge for FilteringCollector {
    fn merge(&mut self, other: Self) {
        self.entries.extend_from_slice(&other.entries);
    }
}

impl Collectable for FilteringCollector {}

/// Helper structure for saving the data to a file.
#[derive(Debug)]
pub struct FilteringSaver;

impl FilteringSaver {
    /// Constructs a new `FilteringSaver`.
    pub fn new() -> Self {
        Self
    }

    /// Saves the result into files.
    #[allow(clippy::unused_self)]
    fn save(
        &self,
        collector: &FilteringCollector,
        config: &config::FilteringConfig,
    ) -> Result<(), errors::ProcessingError> {
        log::info!("Saving {} entries", collector.entries.len());
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&config.wikidata_filtered_dump_path)?;
        for line in &collector.entries {
            file.write_all(line.as_bytes())?;
            file.write_all(b"\n")?;
        }

        Ok(())
    }
}

/// Filters manufacturer entries out from the wikidata dump file.
#[derive(Clone, Debug)]
pub struct FilteringProcessor {
    /// File saver shared between thread.
    saver: std::sync::Arc<std::sync::Mutex<FilteringSaver>>,
}

impl FilteringProcessor {
    /// Constructs a new `FilteringProcessor`.
    pub fn new() -> Self {
        Self { saver: std::sync::Arc::new(std::sync::Mutex::new(FilteringSaver::new())) }
    }

    /// Decides if the passed item should be kept or filtered out.
    ///
    /// The item is kept if it:
    /// - is a product or
    /// - is a manufacturer.
    fn should_keep(item: &Item, sources: &sources::FullSources) -> bool {
        sources.is_product(item) || sources.is_organisation(item)
    }

    /// Saves the result into files.
    fn save(
        &self,
        collector: &FilteringCollector,
        config: &config::FilteringConfig,
    ) -> Result<(), errors::ProcessingError> {
        let saver = self.saver.lock()?;
        saver.save(collector, config)
    }
}

impl Processor for FilteringProcessor {
    type Config = config::FilteringConfig;
    type Essentials = FilteringEssentials;
    type Sources = sources::FullSources;
    type Collector = FilteringCollector;

    fn initialize(
        &self,
        _sources: &Self::Sources,
        _collector: &mut Self::Collector,
        _config: &Self::Config,
    ) -> Result<(), errors::ProcessingError> {
        Ok(())
    }

    /// Handles one Wikidata entity.
    fn handle_entity(
        &self,
        msg: &str,
        entity: &Entity,
        sources: &Self::Sources,
        collector: &mut Self::Collector,
        config: &Self::Config,
    ) -> Result<(), errors::ProcessingError> {
        match entity {
            Entity::Item(item) => {
                if Self::should_keep(item, sources) {
                    collector.add_entry(msg.to_string());

                    // Periodically save data to file to avoid running out of memory.
                    if collector.is_full() {
                        self.save(collector, config)?;
                        collector.clear();
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
        config: &Self::Config,
    ) -> Result<(), errors::ProcessingError> {
        self.save(collector, config)
    }
}
