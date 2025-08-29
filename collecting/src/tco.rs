// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

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
        #[serde(
            rename = "wiki",
            deserialize_with = "transpaer_wikidata::data::Id::deserialize_from_string"
        )]
        pub wikidata_id: transpaer_wikidata::data::Id,
    }
}

/// Reader to loading TCO data.
pub mod reader {
    use super::data::Entry;
    use crate::errors::{IoOrSerdeError, MapIo, MapSerde};

    /// Loads the TCO data from a file.
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
