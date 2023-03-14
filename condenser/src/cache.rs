//! Contains code ralated to parsing and saving cache data.

use serde::{Deserialize, Serialize};

use consumers_collecting::errors::IoOrSerdeError;

use crate::{config::Config, data_collector::DataCollector};

/// Cached data from search over Wikidata data.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Wikidata {
    /// Manufacturer IDs.
    manufacturer_ids: Vec<String>,
}

impl Wikidata {
    /// Checks if the passed ID belongs to a known manufacturer.
    pub fn has_manufacturer_id(&self, id: &String) -> bool {
        self.manufacturer_ids.contains(id)
    }
}

/// Reads in whole saved cache from the previous run.
pub struct Loader {
    config: Config,
}

impl Loader {
    /// Constructs a new `Loader`.
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// Reads in the cache data.
    pub fn load(&self) -> Result<Wikidata, IoOrSerdeError> {
        if self.config.wikidata_input_cache_path.exists() {
            let contents = std::fs::read_to_string(&self.config.wikidata_input_cache_path)?;
            let cache = serde_json::from_str(&contents)?;
            Ok(cache)
        } else {
            Ok(Wikidata::default())
        }
    }
}

/// Writes the new cache from the current run for use in the next run.
pub struct Saver {
    config: Config,
}

impl Saver {
    /// Constructs a new `Saver`.
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// Writes the cache data.
    pub fn save(&self, collector: &DataCollector) -> Result<(), IoOrSerdeError> {
        let cache = Wikidata {
            manufacturer_ids: collector
                .get_manufacturer_ids()
                .iter()
                .map(|id| id.as_string().clone())
                .collect(),
        };

        let contents = serde_json::to_string_pretty(&cache)?;
        std::fs::write(&self.config.wikidata_output_cache_path, contents)?;
        Ok(())
    }
}
