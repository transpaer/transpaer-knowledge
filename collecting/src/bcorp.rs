/// Data structures for parsing `BCorp` data.
pub mod data {
    use serde::{Deserialize, Serialize};

    /// Status of a `BCorp`.
    #[derive(Serialize, Deserialize, Debug)]
    pub enum Status {
        #[serde(rename = "certified")]
        Certified,

        #[serde(rename = "de-certified")]
        Decertified,
    }

    /// Record in a `BCorp` data.
    #[derive(Serialize, Deserialize, Debug)]
    pub struct Record {
        /// Company ID.
        pub company_id: String,

        /// Company name.
        pub company_name: String,

        /// Current status.
        pub current_status: Status,

        /// Company country of origin.
        pub country: String,

        /// Official website URL.
        pub website: String,
    }
}

/// Reader to loading `BCorp` data.
pub mod reader {
    use super::data::Record;
    use crate::errors::{IoOrSerdeError, MapSerde};

    /// Loads the `BCorp` data from a file.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub fn parse(path: &std::path::Path) -> Result<Vec<Record>, IoOrSerdeError> {
        let mut parsed = Vec::<Record>::new();
        let mut reader = csv::Reader::from_path(path).map_with_path(path)?;
        for result in reader.deserialize() {
            parsed.push(result.map_with_path(path)?);
        }
        Ok(parsed)
    }
}
