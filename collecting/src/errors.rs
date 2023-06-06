use thiserror::Error;

pub use sustainity_wikidata::errors::ParseIdError;

/// Error returned when a problem with IO or file parsing occured.
#[derive(Error, Debug)]
pub enum IoOrSerdeError {
    #[error("IO error: {0}")]
    Io(std::io::Error),

    #[error("CSV parsing error: {0}")]
    Csv(csv::Error),

    #[error("JSON parsing error: {0}")]
    Json(serde_json::Error),

    #[error("YAML parsing error: {0}")]
    Yaml(serde_yaml::Error),
}

impl From<std::io::Error> for IoOrSerdeError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<csv::Error> for IoOrSerdeError {
    fn from(error: csv::Error) -> Self {
        Self::Csv(error)
    }
}

impl From<serde_json::Error> for IoOrSerdeError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

impl From<serde_yaml::Error> for IoOrSerdeError {
    fn from(error: serde_yaml::Error) -> Self {
        Self::Yaml(error)
    }
}
