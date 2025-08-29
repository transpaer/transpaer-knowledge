// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

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
        #[serde(
            rename = "wiki",
            deserialize_with = "transpaer_wikidata::data::deserialize_option_id_from_option_string"
        )]
        pub wikidata_id: Option<transpaer_wikidata::data::Id>,

        /// Score of the company in the Fashion Transparency Index.
        #[serde(rename = "score")]
        pub score: i32,
    }
}

/// Reader to loading TCO data.
pub mod reader {
    use super::data::Entry;
    use crate::errors::{IoOrSerdeError, MapIo, MapSerde};

    /// Loads the Fashion Transparency Index data from a file.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub fn parse(path: &std::path::Path) -> Result<Vec<Entry>, IoOrSerdeError> {
        let contents = std::fs::read_to_string(path).map_with_path(path)?;
        let parsed: Vec<Entry> = serde_yaml::from_str(&contents).map_with_path(path)?;
        Ok(parsed)
    }
}
