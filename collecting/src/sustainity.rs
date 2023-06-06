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
}

/// Reader to loading sustainity data.
pub mod reader {
    use super::data::LibraryInfo;
    use crate::errors::IoOrSerdeError;

    /// Loads the sustainity data from a file.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub fn parse<P: AsRef<std::path::Path>>(path: P) -> Result<Vec<LibraryInfo>, IoOrSerdeError> {
        let contents = std::fs::read_to_string(path)?;
        let parsed: Vec<LibraryInfo> = serde_yaml::from_str(&contents)?;
        Ok(parsed)
    }
}
