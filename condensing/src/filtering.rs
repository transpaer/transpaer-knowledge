use std::io::Write;

use sustainity_wikidata::data::{Entity, Item};

use crate::{
    config, errors,
    processing::{Collectable, Processor},
    runners, sources,
};

/// Data storage for gathered data.
///
/// Allows merging different instances.
#[derive(Default, Debug, Clone)]
pub struct FilteringCollector {
    /// Picked entries from wikidata.
    entries: Vec<String>,
}

impl FilteringCollector {
    pub fn add_entry(&mut self, entry: String) {
        self.entries.push(entry);
    }

    #[must_use]
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
#[derive(Debug, Default)]
pub struct FilteringSaver;

impl FilteringSaver {
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

impl Default for FilteringProcessor {
    fn default() -> Self {
        Self { saver: std::sync::Arc::new(std::sync::Mutex::new(FilteringSaver)) }
    }
}

impl Processor for FilteringProcessor {
    type Config = config::FilteringConfig;
    type Sources = sources::FullSources;
    type Collector = FilteringCollector;

    fn finalize(
        &self,
        collector: Self::Collector,
        _sources: &Self::Sources,
        config: &Self::Config,
    ) -> Result<(), errors::ProcessingError> {
        self.save(&collector, config)
    }
}

impl runners::WikidataProcessor for FilteringProcessor {
    fn process_wikidata_entity(
        &self,
        msg: &str,
        entity: Entity,
        sources: &Self::Sources,
        collector: &mut Self::Collector,
        config: &Self::Config,
    ) -> Result<(), errors::ProcessingError> {
        match entity {
            Entity::Item(item) => {
                if Self::should_keep(&item, sources) {
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
}

pub type FilteringRunner = runners::WikidataRunner<FilteringProcessor>;
