use clap::Parser;
use thiserror::Error;

/// Program arguments.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Input data directory.
    #[arg(short, long)]
    input_data: String,

    /// Output data directory.
    #[arg(short, long)]
    output_data: String,

    /// Input cache directory.
    #[arg(short, long)]
    input_cache: String,

    /// Output cache directory.
    #[arg(short, long)]
    output_cache: String,
}

/// Error returned id config checking failed.
#[derive(Error, Debug)]
pub enum ConfigCheckError {
    #[error("Path '{0}' is not a directory")]
    PathIsNotADirectory(std::path::PathBuf),

    #[error("Directory '{0}' does not exist")]
    DirectoryDoesNotExist(std::path::PathBuf),

    #[error("Cannot read directory '{0}'")]
    CannotReadDirectory(std::path::PathBuf),

    #[error("Directory '{0}' is not empty")]
    DirectoryIsNotEmpty(std::path::PathBuf),
}

/// Configuration of the program.
#[derive(Parser, Debug, Clone)]
pub struct Config {
    /// Input data directory.
    input_data: std::path::PathBuf,

    /// Input cache directory.
    input_cache: std::path::PathBuf,

    /// Output data directory.
    output_data: std::path::PathBuf,

    /// Output data directory.
    output_cache: std::path::PathBuf,

    /// Path to Wikidata data.
    pub wikidata_dump_path: std::path::PathBuf,

    /// Path to output Wikidata cache.
    pub wikidata_output_cache_path: std::path::PathBuf,

    /// Path to input Wikidata cache.
    pub wikidata_input_cache_path: std::path::PathBuf,

    /// Path to BCorp data.
    pub bcorp_path: std::path::PathBuf,

    /// Path to TCO data.
    pub tco_path: std::path::PathBuf,

    /// Path to internaly created data.
    pub consumers_path: std::path::PathBuf,

    /// Path to the output product file.
    pub products_target_path: std::path::PathBuf,

    /// Path to the output manufacturers file.
    pub manufacturers_target_path: std::path::PathBuf,
}

impl Config {
    //i/ Constructs a new `Config`.
    pub fn new(args: Args) -> Config {
        let input_data = std::path::PathBuf::from(&args.input_data);
        let input_cache = std::path::PathBuf::from(&args.input_cache);
        let output_data = std::path::PathBuf::from(&args.output_data);
        let output_cache = std::path::PathBuf::from(&args.output_cache);
        Self {
            wikidata_dump_path: input_data.join("wikidata.json.gz"),
            wikidata_input_cache_path: input_cache.join("wikidata.json"),
            wikidata_output_cache_path: output_cache.join("wikidata.json"),
            bcorp_path: input_data.join("bcorp.csv"),
            tco_path: input_data.join("tco.yaml"),
            consumers_path: input_data.join("consumers.yaml"),
            products_target_path: output_data.join("products.json"),
            manufacturers_target_path: output_data.join("manufacturers.json"),
            input_data: input_data,
            input_cache: input_cache,
            output_data: output_data,
            output_cache: output_cache,
        }
    }

    /// Constructs a new config from `Args::parse()`.
    pub fn new_from_args() -> Config {
        Config::new(Args::parse())
    }

    /// Checks validity of the configuration.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        // Output should be an mepty directory
        if !self.output_data.exists() {
            return Err(ConfigCheckError::DirectoryDoesNotExist(self.output_data.clone()));
        }
        if !self.output_data.is_dir() {
            return Err(ConfigCheckError::PathIsNotADirectory(self.output_data.clone()));
        }
        match self.output_data.read_dir() {
            Ok(entries) => {
                if entries.count() != 0 {
                    return Err(ConfigCheckError::DirectoryIsNotEmpty(self.output_data.clone()));
                }
            }
            Err(_) => {
                return Err(ConfigCheckError::CannotReadDirectory(self.output_data.clone()));
            }
        }

        // Success
        Ok(())
    }
}
