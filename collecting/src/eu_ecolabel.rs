/// Data structures for parsing Eu Ecolabel data.
pub mod data {
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
    pub enum ProductOrService {
        #[serde(rename = "PRODUCT")]
        Product,
        #[serde(rename = "SERVICE")]
        Service,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
    #[serde(untagged)]
    pub enum StringOrNumber {
        S(String),
        N(usize),
    }

    #[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
    #[serde(tag = "code_type", content = "code_value")]
    pub enum Code {
        #[serde(rename = "EAN13")]
        Ean13(usize),

        #[serde(rename = "GTIN14")]
        Gtin14(usize),

        #[serde(rename = "Internal Producer ID")]
        Internal(StringOrNumber),

        #[serde(rename = "Other")]
        Other(StringOrNumber),
    }

    /// Record in a EU Ecolabel data.
    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct Record {
        pub product_or_service: ProductOrService,
        pub licence_number: String,
        pub group_name: String,
        #[serde(flatten)]
        pub code: Option<Code>,
        pub product_or_service_name: String,
        pub decision: String,
        pub expiration_date: String,
        pub company_name: String,
        pub company_country: String,
        pub vat_number: Option<String>,
        pub extract_date: String,
    }

    impl Record {
        /// Prepares VAT numebr for easy comparison.
        ///
        /// - ensures country prefix is always present
        /// - the number does not contains special sympbols like " ", "." or "-"
        #[must_use]
        pub fn prepare_vat_number(&self) -> Option<String> {
            if let Some(vat_number) = &self.vat_number {
                let vat_number = vat_number.replace([' ', '.', '-'], "");
                if vat_number.starts_with(&self.company_country) {
                    Some(vat_number)
                } else {
                    Some(format!("{}{}", self.company_country, vat_number))
                }
            } else {
                None
            }
        }
    }
}

/// Reader to loading EU Ecolabel data.
pub mod reader {
    use super::data::Record;
    use crate::errors::{IoOrSerdeError, MapSerde};

    /// Loads the EU Ecolabel data from a file synchroneusly.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub fn parse(path: &std::path::Path) -> Result<Vec<Record>, IoOrSerdeError> {
        let mut parsed = Vec::<Record>::new();
        let mut reader =
            csv::ReaderBuilder::new().delimiter(b';').from_path(path).map_with_path(path)?;
        for result in reader.deserialize() {
            parsed.push(result.map_with_path(path)?);
        }
        Ok(parsed)
    }

    /// Loads the EU Ecolabel data from a file asynchroneusly.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub async fn load<C, F>(path: std::path::PathBuf, callback: C) -> Result<usize, IoOrSerdeError>
    where
        C: Fn(csv::StringRecord, csv::StringRecord) -> F,
        F: std::future::Future<Output = ()>,
    {
        let mut result: usize = 0;
        let mut reader =
            csv::ReaderBuilder::new().delimiter(b';').from_path(&path).map_with_path(&path)?;
        let headers = reader.headers().map_with_path(&path)?.clone();
        for record in reader.into_records() {
            callback(headers.clone(), record.map_with_path(&path)?).await;
            result += 1;
        }
        Ok(result)
    }
}
