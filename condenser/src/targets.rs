use crate::{config::Config, data_collector::DataCollector, knowledge};

/// Writer of the output data.
pub struct TargetWriter {
    config: Config,
}

impl TargetWriter {
    /// Constructs a new `TargetWriter`.
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// Writes the data to files.
    pub fn write(&self, collector: &DataCollector) -> Result<(), std::io::Error> {
        let contents = serde_json::to_string_pretty(&collector.get_products()).unwrap();
        std::fs::write(&self.config.products_target_path, contents)?;

        let manufacturers: Vec<&knowledge::Manufacturer> =
            collector.get_manufacturers().values().collect();
        let contents = serde_json::to_string_pretty(&manufacturers).unwrap();
        std::fs::write(&self.config.manufacturers_target_path, contents)?;

        Ok(())
    }
}
