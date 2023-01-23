/// Data structures for parsing BCorp data.
pub mod data {
    use serde::{Deserialize, Serialize};

    /// Status of a BCorp.
    #[derive(Serialize, Deserialize, Debug)]
    pub enum Status {
        #[serde(rename = "certified")]
        Certified,

        #[serde(rename = "de-certified")]
        Decertified,
    }

    /// Record in a BCorp data.
    #[derive(Serialize, Deserialize, Debug)]
    pub struct Record {
        /// Company ID.
        pub company_id: String,

        /// Company name.
        pub company_name: String,

        /// Current status.
        pub current_status: Status,

        /// Official website URL.
        pub website: String,
    }
}

/// Reder to loading BCorp data.
pub mod reader {
    use crate::bcorp::data::Record;

    /// Loads the BCorp data from a file.
    pub fn parse<P: AsRef<std::path::Path>>(path: P) -> Result<Vec<Record>, std::io::Error> {
        let mut parsed = Vec::new();
        let mut reader = csv::Reader::from_path(path).unwrap();
        for result in reader.deserialize() {
            let record: Record = result.unwrap();
            parsed.push(record);
        }
        Ok(parsed)
    }
}
