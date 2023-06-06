/// Data structures for parsing sustainity data.
pub mod data {
    use serde::{Deserialize, Serialize};

    /// Sustainity topic entry.
    #[derive(Serialize, Deserialize, Debug)]
    pub struct LibraryInfo {
        #[serde(rename = "id")]
        pub id: String,

        #[serde(rename = "title")]
        pub title: String,

        #[serde(rename = "article")]
        pub article: String,
    }

    /// Mapping connecting company or product name to curresponding Wikidata ID.
    #[derive(Serialize, Deserialize, Debug)]
    pub struct NameMatching {
        /// Company or product name.
        #[serde(rename = "name")]
        pub name: String,

        /// Wikidata IDs.
        #[serde(rename = "ids")]
        pub ids: Vec<String>,

        /// Measure of certainty that the matched IDs really belong to the correct entry.
        #[serde(rename = "similarity")]
        pub similarity: f64,
    }

    impl NameMatching {
        /// Check if match has high enough similarity.
        #[must_use]
        pub fn found(&self) -> bool {
            self.similarity > 0.85 && self.ids.len() == 1
        }
    }
}

/// Reader to loading sustainity data.
pub mod reader {
    use super::data::{LibraryInfo, NameMatching};
    use crate::errors::IoOrSerdeError;

    /// Loads the sustainity library data from a file.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub fn parse_library<P: AsRef<std::path::Path>>(
        path: P,
    ) -> Result<Vec<LibraryInfo>, IoOrSerdeError> {
        let contents = std::fs::read_to_string(path)?;
        let parsed: Vec<LibraryInfo> = serde_yaml::from_str(&contents)?;
        Ok(parsed)
    }

    /// Loads a mapping from company or product name to corresponding Wikidata ID..
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub fn parse_id_map<P: AsRef<std::path::Path>>(
        path: P,
    ) -> Result<Vec<NameMatching>, IoOrSerdeError> {
        let contents = std::fs::read_to_string(path)?;
        let parsed: Vec<NameMatching> = serde_yaml::from_str(&contents)?;
        Ok(parsed)
    }
}
