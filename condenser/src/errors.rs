use thiserror::Error;

use consumers_collecting::errors::IoOrSerdeError;
use consumers_wikidata::dump::IoOrChannelError;

/// Error returned when a problem with processing.
#[derive(Error, Debug)]
pub enum ProcessingError {
    #[error("IO error: {0}")]
    Io(std::io::Error),

    #[error("Csv parsing error: {0}")]
    Csv(csv::Error),

    #[error("JSON parsing error: {0}")]
    Json(serde_json::Error),

    #[error("YAML parsing error: {0}")]
    Yaml(serde_yaml::Error),

    #[error("Task joining error: {0}")]
    Join(tokio::task::JoinError),

    #[error("Channel sending error: {0}")]
    Channel(async_channel::SendError<std::string::String>),
}

impl From<std::io::Error> for ProcessingError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<csv::Error> for ProcessingError {
    fn from(error: csv::Error) -> Self {
        Self::Csv(error)
    }
}

impl From<serde_json::Error> for ProcessingError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

impl From<serde_yaml::Error> for ProcessingError {
    fn from(error: serde_yaml::Error) -> Self {
        Self::Yaml(error)
    }
}

impl From<tokio::task::JoinError> for ProcessingError {
    fn from(error: tokio::task::JoinError) -> Self {
        Self::Join(error)
    }
}

impl From<async_channel::SendError<std::string::String>> for ProcessingError {
    fn from(error: async_channel::SendError<std::string::String>) -> Self {
        Self::Channel(error)
    }
}

impl From<IoOrSerdeError> for ProcessingError {
    fn from(error: consumers_collecting::errors::IoOrSerdeError) -> Self {
        match error {
            IoOrSerdeError::Io(error) => Self::Io(error),
            IoOrSerdeError::Csv(error) => Self::Csv(error),
            IoOrSerdeError::Json(error) => Self::Json(error),
            IoOrSerdeError::Yaml(error) => Self::Yaml(error),
        }
    }
}

impl From<IoOrChannelError> for ProcessingError {
    fn from(error: consumers_wikidata::dump::IoOrChannelError) -> Self {
        match error {
            IoOrChannelError::Io(error) => Self::Io(error),
            IoOrChannelError::Channel(error) => Self::Channel(error),
        }
    }
}
