/// Data structures for parsing Fashion Transparency Index data.
pub mod data {
    use serde::{Deserialize, Serialize};

    /// Record in Fashion Transparency Index data.
    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct Entry {
        /// Name of the company.
        #[serde(rename = "name")]
        pub name: String,

        /// ID of the copmpany in Wikidata.
        #[serde(rename = "wiki")]
        pub wikidata_id: Option<sustainity_wikidata::data::Id>,

        /// Score of the company in the Fashion Transparency Index.
        #[serde(rename = "score")]
        pub score: i32,
    }
}

/// Reader to loading TCO data.
pub mod reader {
    use super::data::Entry;
    use crate::errors::{IoOrSerdeError, MapSerde};

    /// Loads the Fashion Transparency Index data from a file.
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
