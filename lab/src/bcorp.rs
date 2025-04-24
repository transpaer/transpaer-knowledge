/// Data structures for parsing `BCorp` support data.
pub mod data {
    use serde::{Deserialize, Serialize};

    /// Country entry for mapping country names to codes.
    #[derive(Serialize, Deserialize, Debug)]
    pub struct Country {
        // BCorp name of the country
        pub country: String,

        /// Country code
        #[serde(deserialize_with = "crate::utils::deserialize_country_code_from_alpha3")]
        pub code: isocountry::CountryCode,
    }

    /// Full structure of the support data.
    #[derive(Serialize, Deserialize, Debug)]
    pub struct Data {
        /// Country data
        pub countries: Vec<Country>,
    }
}

/// Reader to loading `BCorp` data.
pub mod reader {
    use sustainity_collecting::errors::{IoOrSerdeError, MapSerde};

    use super::data::Data;

    /// Loads the `BCorp` support data from a file.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub fn parse(path: &std::path::Path) -> Result<Data, IoOrSerdeError> {
        let contents = std::fs::read_to_string(path)?;
        let parsed: Data = serde_yaml::from_str(&contents).map_with_path(path)?;
        Ok(parsed)
    }
}
