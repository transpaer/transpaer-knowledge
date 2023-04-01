use crate::{config::Config, data_collector::DataCollector, knowledge};

use consumers_collecting::errors::IoOrSerdeError;

/// Writer of the output data.
pub struct TargetWriter {
    config: Config,
}

impl TargetWriter {
    /// Constructs a new `TargetWriter`.
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// Writes the collected data to files.
    pub fn write_collected_data(&self, collector: &DataCollector) -> Result<(), IoOrSerdeError> {
        let contents = serde_json::to_string_pretty(&collector.get_products())?;
        std::fs::write(&self.config.products_target_path, contents)?;

        let manufacturers: Vec<&knowledge::Manufacturer> =
            collector.get_manufacturers().values().collect();
        let contents = serde_json::to_string_pretty(&manufacturers)?;
        std::fs::write(&self.config.manufacturers_target_path, contents)?;

        Ok(())
    }

    /// Writes the topic info data to files.
    pub fn write_info_data(&self, info: &[knowledge::Info]) -> Result<(), IoOrSerdeError> {
        let contents = serde_json::to_string_pretty(info)?;
        std::fs::write(&self.config.info_target_path, contents)?;

        Ok(())
    }
}
