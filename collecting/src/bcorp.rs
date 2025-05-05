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

    impl Status {
        #[must_use]
        pub fn is_certified(&self) -> bool {
            match self {
                Self::Certified => true,
                Self::Decertified => false,
            }
        }
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

        /// Description.
        pub description: String,

        /// Data of last certification.
        ///
        /// The data contains multiple records for the same company. Only the latest one is relevant.
        #[serde(with = "super::serde")]
        pub date_certified: chrono::DateTime<chrono::Utc>,

        /// Company country of origin.
        pub country: String,

        /// Official website URL.
        pub website: String,
    }
}

mod serde {
    use chrono::{DateTime, NaiveDateTime, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

    const FORMAT1: &str = "%Y-%m-%d %H:%M:%S%:z";
    const FORMAT2: &str = "%Y-%m-%d %H:%M:%S.%f%:z";

    pub fn serialize<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", date.format(FORMAT1));
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let dt = NaiveDateTime::parse_from_str(&s, FORMAT1)
            .or_else(|_| NaiveDateTime::parse_from_str(&s, FORMAT2))
            .map_err(serde::de::Error::custom)?;
        Ok(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
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
