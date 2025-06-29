use thiserror::Error;

pub use sustainity_collecting::errors::IoOrSerdeError;
pub use sustainity_models::buckets::BucketError;
pub use sustainity_wikidata::dump::LoaderError;

use crate::{coagulate::ExternalId, substrate::DataSetId, wikidata::WikiId};

/// Error returned if config checking failed.
#[derive(Error, Debug)]
pub enum ConfigCheckError {
    #[error("Path '{0}' does not exist")]
    DoesNotExist(std::path::PathBuf),

    #[error("Path '{0}' is not a file")]
    NotAFile(std::path::PathBuf),

    #[error("Path '{0}' is not a directory")]
    NotADir(std::path::PathBuf),

    #[error("Path '{0}' is not a readable")]
    NotReadable(std::path::PathBuf),

    #[error("Path '{0}' is not an empty directory")]
    NotAnEmptyDir(std::path::PathBuf),

    #[error("Path '{0}' already exists")]
    AlreadyExists(std::path::PathBuf),

    #[error("Path '{0}' has no parent")]
    NoParent(std::path::PathBuf),
}

/// Error related to validating the input data.
#[derive(Error, Debug)]
pub enum SourcesCheckError {
    /// IDs were duplicated while expected to be unique.
    #[error("Repeated IDs: {0:?}")]
    RepeatedIds(std::collections::HashSet<WikiId>),
}

/// Errors specific to the `condense` command.
#[derive(Error, Debug)]
pub enum CondensationError {
    #[error("IO error: {0} ({1:?})")]
    Io(std::io::Error, std::path::PathBuf),

    #[error("Saving Substrate error: {0}")]
    WriteSubstrate(#[from] sustainity_schema::errors::SaveError),
}

/// Errors specific to the crystalisation command.
#[derive(Error, Debug)]
pub enum CoagulationError {
    #[error("Reading substrate: {0}")]
    ReadSubstrate(#[from] sustainity_schema::errors::ReadError),

    #[error("Unique ID not found for: {inner_id:?} while {when} in {data_set_path:?}")]
    UniqueIdNotFoundForInnerId { inner_id: String, data_set_path: std::path::PathBuf, when: String },

    #[error("Unique ID {id:?} repeated")]
    ExternalIdRepeated { id: ExternalId },

    #[error("Substrate name not found for data set `{id:?}`")]
    SubstrateNameNotFoundForId { id: DataSetId },

    #[error("Substrate ID not found for data set `{name}`")]
    SubstrateIdNotFoundForName { name: String },

    #[error("IO error: {0} ({1:?})")]
    Io(std::io::Error, std::path::PathBuf),

    #[error("IO or serde error: {0}")]
    IoOrSerde(#[from] IoOrSerdeError),

    #[error("Bucket: {0}")]
    Bucket(#[from] BucketError),
}

/// Errors specific to the crystalisation command.
#[derive(Error, Debug)]
pub enum CrystalizationError {
    #[error("Crystalization: {0:?}")]
    ReadSubstrate(#[from] sustainity_schema::errors::ReadError),

    #[error("ISO country code while (when {when}): {source}")]
    IsoCountry { source: isocountry::CountryCodeParseErr, when: &'static str },

    #[error("Bocket: {0}")]
    Bucket(#[from] BucketError),

    #[error("Keys are not unique for: {comment} (only {unique} unique out of {all})")]
    NotUniqueKeys { comment: String, unique: usize, all: usize },

    // TODO: Inline the variants
    #[error("Coagulation error: {0}")]
    Coagulation(#[from] CoagulationError),
}

// TODO: Ideally this type could be removed.
/// Error returned when a problem with processing.
#[derive(Error, Debug)]
pub enum ProcessingError {
    #[error("In file `{1}`.\nIO error: {0}")]
    Io(std::io::Error, std::path::PathBuf),

    #[error("IO error while spawning a tread: {0}")]
    Thread(std::io::Error),

    #[error("In file `{1}`.\nCSV parsing error: {0}")]
    ReadCsv(csv::Error, std::path::PathBuf),

    #[error("In file `{1}`.\nJSON parsing error: {0}")]
    ReadJson(serde_json::Error, std::path::PathBuf),

    #[error("In file `{1}`.\nYAML parsing error: {0}")]
    ReadYaml(serde_yaml::Error, std::path::PathBuf),

    #[error("Reading Substrate error: {0}")]
    ReadSubstrate(#[from] sustainity_schema::errors::ReadError),

    #[error("CSV serialization error: {0}")]
    WriteCsv(csv::Error),

    #[error("JSON serialization error: {0}")]
    WriteJson(serde_json::Error),

    #[error("YAML serialization error: {0}")]
    WriteYaml(serde_yaml::Error),

    #[error("Saving Substrate error: {0}")]
    WriteSubstrate(#[from] sustainity_schema::errors::SaveError),

    #[error("Bucket: {0}")]
    Bucket(#[from] BucketError),

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

    #[error("Crystalization error: {0}")]
    Condensation(#[from] CondensationError),

    #[error("Crystalization error: {0}")]
    Crystalization(#[from] CrystalizationError),

    #[error("Coagulation error: {0}")]
    Coagulation(#[from] CoagulationError),

    #[error("ID parsing: {0}")]
    IdParsing(#[from] sustainity_models::ids::ParseIdError),

    #[error("Wikidata ID parsing: {0}")]
    WikiIdParsing(#[from] sustainity_wikidata::errors::ParseIdError),

    #[error("Mutex lock")]
    MutexLock,

    #[error("After processing the data collector was empty")]
    EmptyCollector,
}

impl<T> From<std::sync::PoisonError<T>> for ProcessingError {
    fn from(_error: std::sync::PoisonError<T>) -> Self {
        Self::MutexLock
    }
}

impl From<IoOrSerdeError> for ProcessingError {
    fn from(error: IoOrSerdeError) -> Self {
        match error {
            IoOrSerdeError::Io(error, path) => Self::Io(error, path),
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
            LoaderError::Io(source, path) => Self::Io(source, path),
            LoaderError::CompressionMethod => Self::CompressionMethod,
        }
    }
}
