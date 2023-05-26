use clap::{Parser, Subcommand};

use crate::{errors::CheckError, utils};

/// Arguments of the `prefilter` command.
#[derive(Parser, Debug)]
pub struct PrefilteringArgs {
    /// Input data directory.
    #[arg(long)]
    input_data: String,

    /// Output cache directory.
    #[arg(long)]
    output_cache: String,
}

/// Arguments of the `filter` command.
#[derive(Parser, Debug)]
pub struct FilteringArgs {
    /// Input data directory.
    #[arg(long)]
    input_data: String,

    /// Input cache directory.
    #[arg(long)]
    input_cache: String,

    /// Output cache directory.
    #[arg(long)]
    output_cache: String,
}

/// Arguments of the `condense` command.
#[derive(Parser, Debug)]
pub struct CondensationArgs {
    /// Input data directory.
    #[arg(long)]
    input_data: String,

    /// Output data directory.
    #[arg(long)]
    output_data: String,

    /// Input cache directory.
    #[arg(long)]
    input_cache: String,
}

/// Arguments of the `transcribe` command.
#[derive(Parser, Debug)]
pub struct TranscriptionArgs {
    /// Input data directory.
    #[arg(long)]
    input_data: String,

    /// Output data directory.
    #[arg(long)]
    output_data: String,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    Prefilter(PrefilteringArgs),
    Filter(FilteringArgs),
    Condense(CondensationArgs),
    Transcribe(TranscriptionArgs),
}

/// Program arguments.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Commands.
    #[command(subcommand)]
    pub command: Commands,
}

/// Configuration for the `filter-products` command.
#[derive(Debug, Clone)]
pub struct PrefilteringConfig {
    /// Path to input Wikidata data.
    pub wikidata_dump_path: std::path::PathBuf,

    /// Path to output Wikidata cache.
    pub wikidata_cache_path: std::path::PathBuf,
}

impl PrefilteringConfig {
    /// Constructs a new `Prefiltering`.
    pub fn new(args: &PrefilteringArgs) -> PrefilteringConfig {
        let input_data = std::path::PathBuf::from(&args.input_data);
        let output_cache = std::path::PathBuf::from(&args.output_cache);
        Self {
            wikidata_dump_path: input_data.join("wikidata.json.gz"),
            wikidata_cache_path: output_cache.join("wikidata_cache.json"),
        }
    }

    /// Checks validity of the configuration.
    pub fn check(&self) -> Result<(), CheckError> {
        utils::path_exists(&self.wikidata_dump_path)?;
        utils::path_creatable(&self.wikidata_cache_path)?;
        Ok(())
    }
}

/// Configuration for the `filter-manufacturers` command.
#[derive(Debug, Clone)]
pub struct FilteringConfig {
    /// Path to input Wikidata data.
    pub wikidata_full_dump_path: std::path::PathBuf,

    /// Path to output Wikidata cache.
    pub wikidata_cache_path: std::path::PathBuf,

    /// Path to output filtered .
    pub wikidata_filtered_dump_path: std::path::PathBuf,
}

impl FilteringConfig {
    /// Constructs a new `FilteringConfig`.
    pub fn new(args: &FilteringArgs) -> FilteringConfig {
        let input_data = std::path::PathBuf::from(&args.input_data);
        let input_cache = std::path::PathBuf::from(&args.input_cache);
        let output_cache = std::path::PathBuf::from(&args.output_cache);
        Self {
            wikidata_full_dump_path: input_data.join("wikidata.json.gz"),
            wikidata_cache_path: input_cache.join("wikidata_cache.json"),
            wikidata_filtered_dump_path: output_cache.join("wikidata.jsonl"),
        }
    }

    /// Checks validity of the configuration.
    pub fn check(&self) -> Result<(), CheckError> {
        utils::path_exists(&self.wikidata_full_dump_path)?;
        utils::path_exists(&self.wikidata_cache_path)?;
        utils::path_creatable(&self.wikidata_filtered_dump_path)?;
        Ok(())
    }
}

/// Configuration for the `condense` command.
#[derive(Debug, Clone)]
pub struct CondensationConfig {
    /// Path to input wikidata dump.
    pub wikidata_source_path: std::path::PathBuf,

    /// Path to input Wikidata cache.
    pub wikidata_cache_path: std::path::PathBuf,

    /// Path to BCorp data.
    pub bcorp_path: std::path::PathBuf,

    /// Path to TCO data.
    pub tco_path: std::path::PathBuf,

    /// Path to the output product file.
    pub target_products_path: std::path::PathBuf,

    /// Path to the output manufacturers file.
    pub target_manufacturers_path: std::path::PathBuf,
}

impl CondensationConfig {
    /// Constructs a new `CondensationConfig`.
    pub fn new(args: &CondensationArgs) -> CondensationConfig {
        let input_data = std::path::PathBuf::from(&args.input_data);
        let input_cache = std::path::PathBuf::from(&args.input_cache);
        let output_data = std::path::PathBuf::from(&args.output_data);
        Self {
            wikidata_source_path: input_cache.join("wikidata.jsonl"),
            wikidata_cache_path: input_cache.join("wikidata_cache.json"),
            bcorp_path: input_data.join("bcorp.csv"),
            tco_path: input_data.join("tco.yaml"),
            target_products_path: output_data.join("products.json"),
            target_manufacturers_path: output_data.join("manufacturers.json"),
        }
    }

    /// Checks validity of the configuration.
    pub fn check(&self) -> Result<(), CheckError> {
        utils::path_exists(&self.wikidata_source_path)?;
        utils::path_exists(&self.wikidata_cache_path)?;
        utils::path_exists(&self.bcorp_path)?;
        utils::path_exists(&self.tco_path)?;
        utils::path_creatable(&self.target_products_path)?;
        utils::path_creatable(&self.target_manufacturers_path)?;
        Ok(())
    }
}

/// Configuration for the `transcribe` command.
#[derive(Debug, Clone)]
pub struct TranscriptionConfig {
    /// Path to the output info file.
    pub consumers_path: std::path::PathBuf,

    /// Path to the output info file.
    pub target_info_path: std::path::PathBuf,
}

impl TranscriptionConfig {
    //i/ Constructs a new `TranscriptionConfig`.
    pub fn new(args: &TranscriptionArgs) -> TranscriptionConfig {
        let input_data = std::path::PathBuf::from(&args.input_data);
        let output_data = std::path::PathBuf::from(&args.output_data);
        Self {
            consumers_path: input_data.join("consumers.yaml"),
            target_info_path: output_data.join("info.json"),
        }
    }

    /// Checks validity of the configuration.
    pub fn check(&self) -> Result<(), CheckError> {
        utils::path_exists(&self.consumers_path)?;
        utils::path_creatable(&self.target_info_path)?;
        Ok(())
    }
}

/// Configuration for the program.
#[derive(Debug, Clone)]
pub enum Config {
    Prefiltering(PrefilteringConfig),
    Filtering(FilteringConfig),
    Condensation(CondensationConfig),
    Transcription(TranscriptionConfig),
}

impl Config {
    /// Constructs a new config from `Args::parse()`.
    pub fn new_from_args() -> Config {
        let args = Args::parse();
        match args.command {
            Commands::Prefilter(args) => Config::Prefiltering(PrefilteringConfig::new(&args)),
            Commands::Filter(args) => Config::Filtering(FilteringConfig::new(&args)),
            Commands::Condense(args) => Config::Condensation(CondensationConfig::new(&args)),
            Commands::Transcribe(args) => Config::Transcription(TranscriptionConfig::new(&args)),
        }
    }
}
