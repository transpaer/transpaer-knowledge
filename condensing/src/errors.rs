use thiserror::Error;

pub use sustainity_collecting::errors::IoOrSerdeError;
pub use sustainity_wikidata::dump::LoaderError;

use crate::wikidata::WikiId;

/// Error returned if config checking failed.
#[derive(Error, Debug)]
pub enum ConfigCheckError {
    #[error("Path '{0}' does not exist")]
    PathDoesNotExist(std::path::PathBuf),

    #[error("Path '{0}' is not a file")]
    PathIsNotAFile(std::path::PathBuf),

    #[error("Path '{0}' is not a directory")]
    PathIsNotADir(std::path::PathBuf),

    #[error("Path '{0}' already exists")]
    PathAlreadyExists(std::path::PathBuf),

    #[error("Base of '{0}' does not exist")]
    BaseDoesNotExist(std::path::PathBuf),

    #[error("Base of '{0}' is not a directory")]
    BaseIsNotADirectory(std::path::PathBuf),
}

/// Error related to validating the input data.
#[derive(Error, Debug)]
pub enum SourcesCheckError {
    /// IDs were duplicated while expected to be unique.
    #[error("Repeated IDs: {0:?}")]
    RepeatedIds(std::collections::HashSet<WikiId>),
}

/// Error returned when a problem with processing.
#[derive(Error, Debug)]
pub enum ProcessingError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("In file `{1}`.\nCSV parsing error: {0}")]
    ReadCsv(csv::Error, std::path::PathBuf),

    #[error("In file `{1}`.\nJSON parsing error: {0}")]
    ReadJson(serde_json::Error, std::path::PathBuf),

    #[error("In file `{1}`.\nYAML parsing error: {0}")]
    ReadYaml(serde_yaml::Error, std::path::PathBuf),

    #[error("CSV serialization error: {0}")]
    WriteCsv(csv::Error),

    #[error("JSON serialization error: {0}")]
    WriteJson(serde_json::Error),

    #[error("YAML serialization error: {0}")]
    WriteYaml(serde_yaml::Error),

    #[error("Variant parsing error: {0}")]
    Variant(#[from] serde_variant::UnsupportedType),

    #[error("Wrong country code: {0}")]
    CountryCode(#[from] isocountry::CountryCodeParseErr),

    #[error("Unknown compression method")]
    CompressionMethod,

    #[error("Channel sending error: {0}")]
    Channel(#[from] async_channel::SendError<std::string::String>),

    #[error("Config check: {0}")]
    ConfigCheck(#[from] ConfigCheckError),

    #[error("Sources check: {0}")]
    SourcesCheck(#[from] SourcesCheckError),

    #[error("ID parsing: {0}")]
    IdParsing(#[from] sustainity_models::ids::ParseIdError),

    #[error("Wikidata ID parsing: {0}")]
    WikiIdParsing(#[from] sustainity_wikidata::errors::ParseIdError),

    #[error("Mutex lock")]
    MutexLock,
}

impl<T> From<std::sync::PoisonError<T>> for ProcessingError {
    fn from(_error: std::sync::PoisonError<T>) -> Self {
        Self::MutexLock
    }
}

impl From<IoOrSerdeError> for ProcessingError {
    fn from(error: IoOrSerdeError) -> Self {
        match error {
            IoOrSerdeError::Io(error) => Self::Io(error),
            IoOrSerdeError::ReadCsv(error, path) => Self::ReadCsv(error, path),
            IoOrSerdeError::ReadJson(error, path) => Self::ReadJson(error, path),
            IoOrSerdeError::ReadYaml(error, path) => Self::ReadYaml(error, path),
            IoOrSerdeError::WriteCsv(error) => Self::WriteCsv(error),
            IoOrSerdeError::WriteJson(error) => Self::WriteJson(error),
            IoOrSerdeError::WriteYaml(error) => Self::WriteYaml(error),
        }
    }
}

impl From<LoaderError> for ProcessingError {
    fn from(error: LoaderError) -> Self {
        match error {
            LoaderError::Io(error) => Self::Io(error),
            LoaderError::CompressionMethod => Self::CompressionMethod,
        }
    }
}
