/// Data structures for parsing Eu Ecolabel data.
pub mod data {
    use serde::{Deserialize, Serialize};

    /// Record in a EU Ecolabel data.
    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct Record {
        pub product_or_service: String,
        pub licence_number: String,
        pub group_name: String,
        pub code_type: Option<String>,
        pub code_value: Option<String>,
        pub product_or_service_name: String,
        pub decision: String,
        pub expiration_date: String,
        pub company_name: String,
        pub company_country: String,
        pub vat_number: Option<String>,
        pub extract_date: String,
    }
}

/// Reader to loading EU Ecolabel data.
pub mod reader {
    use super::data::Record;
    use crate::errors::IoOrSerdeError;

    /// Loads the EU Ecolabel data from a file.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub fn parse<P: AsRef<std::path::Path>>(path: P) -> Result<Vec<Record>, IoOrSerdeError> {
        let mut parsed = Vec::<Record>::new();
        let mut reader = csv::ReaderBuilder::new().delimiter(b';').from_path(path)?;
        for result in reader.deserialize() {
            parsed.push(result?);
        }
        Ok(parsed)
    }
}
