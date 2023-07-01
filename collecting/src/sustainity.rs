/// Data structures for parsing sustainity data.
pub mod data {
    use serde::{Deserialize, Serialize};

    /// Enumerates all library topics.
    #[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
    pub enum LibraryTopic {
        #[serde(rename = "info:main")]
        InfoMain,

        #[serde(rename = "info:for_producers")]
        InfoForProducers,

        #[serde(rename = "info:faq")]
        InfoFaq,

        #[serde(rename = "data:wiki")]
        DataWiki,

        #[serde(rename = "data:open_food_facts")]
        DataOpenFoodFacts,

        #[serde(rename = "cert:bcorp")]
        CertBcorp,

        #[serde(rename = "cert:eu_ecolabel")]
        CertEuEcolabel,

        #[serde(rename = "cert:tco")]
        CertTco,

        #[serde(rename = "cert:fti")]
        CertFti,

        #[serde(rename = "other:not_found")]
        OtherNotFound,
    }

    /// Sustainity topic entry.
    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct LibraryInfo {
        #[serde(rename = "id")]
        pub id: LibraryTopic,

        #[serde(rename = "title")]
        pub title: String,

        #[serde(rename = "summary")]
        pub summary: String,
    }

    /// Mapping connecting company or product name to curresponding Wikidata ID.
    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct NameMatching {
        /// Company or product name.
        #[serde(rename = "name")]
        pub name: String,

        /// Wikidata IDs.
        #[serde(rename = "ids")]
        pub ids: Vec<sustainity_wikidata::data::StrId>,

        /// Measure of certainty that the matched IDs really belong to the correct entry.
        #[serde(rename = "similarity")]
        pub similarity: f64,
    }

    impl NameMatching {
        /// Check if match has high enough similarity.
        #[must_use]
        pub fn matched(&self) -> Option<sustainity_wikidata::data::StrId> {
            if self.similarity > 0.85 && self.ids.len() == 1 {
                self.ids.first().cloned()
            } else {
                None
            }
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
