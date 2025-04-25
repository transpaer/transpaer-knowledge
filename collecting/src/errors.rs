use thiserror::Error;

pub use sustainity_wikidata::errors::ParseIdError;

/// Error returned when a problem with IO or file parsing occured.
#[derive(Error, Debug)]
pub enum IoOrSerdeError {
    #[error("In file `{1}`.\nIO error: {0}")]
    Io(std::io::Error, std::path::PathBuf),

    #[error("In file `{1}`.\nCSV parsing error: {0}")]
    ReadCsv(csv::Error, std::path::PathBuf),

    #[error("In file `{1}`.\n JSON parsing error: {0}")]
    ReadJson(serde_json::Error, std::path::PathBuf),

    #[error("In file `{1}`.\nYAML parsing error: {0}")]
    ReadYaml(serde_yaml::Error, std::path::PathBuf),

    #[error("CSV serialization error: {0}")]
    WriteCsv(csv::Error),

    #[error("JSON serialization error: {0}")]
    WriteJson(serde_json::Error),

    #[error("YAML serialization error: {0}")]
    WriteYaml(serde_yaml::Error),
}

/// Trait for mapping from IO errors to `IoOrSerdeError`.
#[allow(clippy::missing_errors_doc)]
pub trait MapIo<T> {
    /// Maps `Result` to `Result` with `IoOrSerdeError`.
    fn map_with_path(self, path: &std::path::Path) -> Result<T, IoOrSerdeError>;
}

impl<T> MapIo<T> for Result<T, std::io::Error> {
    fn map_with_path(self, path: &std::path::Path) -> Result<T, IoOrSerdeError> {
        self.map_err(|e| IoOrSerdeError::Io(e, path.into()))
    }
}

/// Trait for mapping from Serde crate errors to `IoOrSerdeError`.
#[allow(clippy::missing_errors_doc)]
pub trait MapSerde<T> {
    /// Maps `Result` to `Result` with `IoOrSerdeError`.
    fn map_serde(self) -> Result<T, IoOrSerdeError>;

    /// Maps `Result` to `Result` with `IoOrSerdeError` adding the path of the file which parsing triggred the error.
    fn map_with_path(self, path: &std::path::Path) -> Result<T, IoOrSerdeError>;
}

impl<T> MapSerde<T> for Result<T, csv::Error> {
    fn map_serde(self) -> Result<T, IoOrSerdeError> {
        self.map_err(IoOrSerdeError::WriteCsv)
    }

    fn map_with_path(self, path: &std::path::Path) -> Result<T, IoOrSerdeError> {
        self.map_err(|e| IoOrSerdeError::ReadCsv(e, path.into()))
    }
}

impl<T> MapSerde<T> for Result<T, serde_json::Error> {
    fn map_serde(self) -> Result<T, IoOrSerdeError> {
        self.map_err(IoOrSerdeError::WriteJson)
    }

    fn map_with_path(self, path: &std::path::Path) -> Result<T, IoOrSerdeError> {
        self.map_err(|e| IoOrSerdeError::ReadJson(e, path.into()))
    }
}

impl<T> MapSerde<T> for Result<T, serde_yaml::Error> {
    fn map_serde(self) -> Result<T, IoOrSerdeError> {
        self.map_err(IoOrSerdeError::WriteYaml)
    }

    fn map_with_path(self, path: &std::path::Path) -> Result<T, IoOrSerdeError> {
        self.map_err(|e| IoOrSerdeError::ReadYaml(e, path.into()))
    }
}
