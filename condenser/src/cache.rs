//! Contains code ralated to parsing and saving cache data.

use serde::{Deserialize, Serialize};

use crate::{config::Config, data_collector::DataCollector};

/// Cached data from search over Wikidata data.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct WikidataCache {
    /// Manufacturer IDs.
    manufacturer_ids: Vec<String>,
}

impl WikidataCache {
    /// Checks if the passed ID belongs to a known manufacturer.
    pub fn has_manufacturer_id(&self, id: &String) -> bool {
        self.manufacturer_ids.contains(id)
    }
}

/// Reads in whole saved cache from the previous run.
pub struct CacheReader {
    config: Config,
}

impl CacheReader {
    /// Constructs a new `CacheReader`.
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// Reads in the cache data.
    pub fn read(&self) -> Result<WikidataCache, std::io::Error> {
        if self.config.wikidata_input_cache_path.exists() {
            let contents = std::fs::read_to_string(&self.config.wikidata_input_cache_path)?;
            let cache = serde_json::from_str(&contents).unwrap();
            Ok(cache)
        } else {
            Ok(WikidataCache::default())
        }
    }
}

/// Writes the new cache from the current run for use in the next run.
pub struct CacheWriter {
    config: Config,
}

impl CacheWriter {
    /// Constructs a new `CacheReader`.
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// Writes the cache data.
    pub fn write(&self, collector: &DataCollector) -> Result<(), std::io::Error> {
        let cache = WikidataCache {
            manufacturer_ids: collector
                .get_manufacturer_ids()
                .iter()
                .map(|id| id.to_string())
                .collect(),
        };

        let contents = serde_json::to_string_pretty(&cache).unwrap();
        std::fs::write(&self.config.wikidata_output_cache_path, &contents)?;
        Ok(())
    }
}
