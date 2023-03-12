use thiserror::Error;

/// Error returned when a problem with IO or file parsing occured.
#[derive(Error, Debug)]
pub enum IoOrParsingError {
    #[error("IO error: {0}")]
    Io(std::io::Error),

    #[error("YAML parsing error: {0}")]
    Yaml(serde_yaml::Error),
}

impl From<std::io::Error> for IoOrParsingError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_yaml::Error> for IoOrParsingError {
    fn from(error: serde_yaml::Error) -> Self {
        Self::Yaml(error)
    }
}
