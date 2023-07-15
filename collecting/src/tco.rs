/// Data structures for parsing TCO data.
pub mod data {
    use serde::{Deserialize, Serialize};

    /// Record in TCO data.
    #[derive(Serialize, Deserialize, Debug)]
    pub struct Entry {
        /// Company name.
        #[serde(rename = "tco")]
        pub company_name: String,

        /// Wikidata ID of the company.
        #[serde(rename = "wiki")]
        pub wikidata_id: sustainity_wikidata::data::StrId,
    }
}

/// Reader to loading TCO data.
pub mod reader {
    use super::data::Entry;
    use crate::errors::{IoOrSerdeError, MapSerde};

    /// Loads the TCO data from a file.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub fn parse(path: &std::path::Path) -> Result<Vec<Entry>, IoOrSerdeError> {
        let contents = std::fs::read_to_string(path)?;
        let parsed: Vec<Entry> = serde_yaml::from_str(&contents).map_with_path(path)?;
        Ok(parsed)
    }
}
