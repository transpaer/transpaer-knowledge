/// Data structures for parsing TCO data.
pub mod data {
    use serde::{Deserialize, Serialize};

    /// Record in a BCorp data.
    #[derive(Serialize, Deserialize, Debug)]
    pub struct Entry {
        /// Company name.
        #[serde(rename = "tco")]
        pub company_name: String,

        /// Wikidata ID of the company.
        #[serde(rename = "wiki")]
        pub wikidata_id: String,
    }
}

/// Reader to loading TCO data.
pub mod reader {
    use super::data::Entry;

    /// Loads the TCO data from a file.
    pub fn parse<P: AsRef<std::path::Path>>(path: P) -> Result<Vec<Entry>, std::io::Error> {
        let contents = std::fs::read_to_string(path)?;
        let parsed: Vec<Entry> = serde_yaml::from_str(&contents).unwrap();
        Ok(parsed)
    }
}
