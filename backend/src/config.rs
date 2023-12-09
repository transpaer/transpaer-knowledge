use serde::{Deserialize, Serialize};
use snafu::prelude::*;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum ConfigError {
    #[snafu(context(false), display("Failed to read config file: {source}"))]
    Io { source: std::io::Error },

    #[snafu(context(false), display("Failed to read config environment variable: {source}"))]
    Env { source: std::env::VarError },

    #[snafu(context(false), display("Failed to parse config: {source}"))]
    Serde { source: serde_json::Error },
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SecretConfig {
    #[serde(rename = "host")]
    pub host: String,

    #[serde(rename = "user")]
    pub user: String,

    #[serde(rename = "password")]
    pub password: String,
}

impl SecretConfig {
    pub fn new(host: String, user: String, password: String) -> Self {
        Self { host, user, password }
    }

    pub fn new_debug() -> Self {
        Self::new("http://localhost:8529".into(), "".into(), "".into())
    }

    pub fn load_from_file() -> Result<Self, ConfigError> {
        const PATH: &str = "/etc/sustainity/secrets/sustainity.json";
        let data = std::fs::read_to_string(PATH)?;
        let config = serde_json::from_str(&data)?;
        Ok(config)
    }

    pub fn load_from_env() -> Result<Self, ConfigError> {
        const ENV: &str = "SUSTAINITY_CONFIG";
        let data = std::env::var(ENV)?;
        let config = serde_json::from_str(&data)?;
        Ok(config)
    }

    pub fn load_or_default() -> Self {
        match Self::load_from_file() {
            Ok(ok) => return ok,
            Err(err) => log::warn!("{err}"),
        }

        match Self::load_from_env() {
            Ok(ok) => return ok,
            Err(err) => log::warn!("{err}"),
        }

        log::info!("Using default config");
        SecretConfig::new_debug()
    }
}
