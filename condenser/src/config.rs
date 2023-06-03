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

/// Arguments of the `analyse` command.
#[derive(Parser, Debug)]
pub struct AnalysisArgs {
    /// Input cache directory.
    #[arg(long)]
    input_cache: String,
}

/// All arguments of the program.
#[derive(Subcommand, Debug)]
pub enum Commands {
    Prefilter(PrefilteringArgs),
    Filter(FilteringArgs),
    Condense(CondensationArgs),
    Transcribe(TranscriptionArgs),
    Analyze(AnalysisArgs),
}

/// Program arguments.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Commands.
    #[command(subcommand)]
    pub command: Commands,
}

/// Subconfiguration related to source files used by several other configs.
#[derive(Debug, Clone)]
pub struct SourcesConfig {
    /// Path to input Wikidata cache.
    pub wikidata_cache_path: std::path::PathBuf,

    /// Path to BCorp data.
    pub bcorp_path: std::path::PathBuf,

    /// Path to TCO data.
    pub tco_path: std::path::PathBuf,

    /// Path to Fashion Transparency Index data.
    pub fashion_transparency_index_path: std::path::PathBuf,
}

impl SourcesConfig {
    /// Constructs a new `CondensationConfig`.
    pub fn new(input_data: &str, input_cache: &str) -> SourcesConfig {
        let input_cache = std::path::PathBuf::from(input_cache);
        let input_data = std::path::PathBuf::from(input_data);
        Self {
            wikidata_cache_path: input_cache.join("wikidata_cache.json"),
            bcorp_path: input_data.join("bcorp.csv"),
            tco_path: input_data.join("tco.yaml"),
            fashion_transparency_index_path: input_data
                .join("sustainity_fashion_transparency_index.yaml"),
        }
    }

    /// Checks validity of the configuration.
    pub fn check(&self) -> Result<(), CheckError> {
        utils::path_exists(&self.wikidata_cache_path)?;
        utils::path_exists(&self.bcorp_path)?;
        utils::path_exists(&self.tco_path)?;
        utils::path_exists(&self.fashion_transparency_index_path)?;
        Ok(())
    }
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
            wikidata_dump_path: input_data.join("wikidata-20230417-all.json.gz"),
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

    /// Path to output filtered .
    pub wikidata_filtered_dump_path: std::path::PathBuf,

    /// Data sources.
    pub sources: SourcesConfig,
}

impl FilteringConfig {
    /// Constructs a new `FilteringConfig`.
    pub fn new(args: &FilteringArgs) -> FilteringConfig {
        let input_data = std::path::PathBuf::from(&args.input_data);
        let output_cache = std::path::PathBuf::from(&args.output_cache);
        Self {
            wikidata_full_dump_path: input_data.join("wikidata-20230417-all.json.gz"),
            wikidata_filtered_dump_path: output_cache.join("wikidata.jsonl"),
            sources: SourcesConfig::new(&args.input_data, &args.input_cache),
        }
    }

    /// Checks validity of the configuration.
    pub fn check(&self) -> Result<(), CheckError> {
        utils::path_exists(&self.wikidata_full_dump_path)?;
        utils::path_creatable(&self.wikidata_filtered_dump_path)?;
        self.sources.check()?;
        Ok(())
    }
}

/// Configuration for the `condense` command.
#[derive(Debug, Clone)]
pub struct CondensationConfig {
    /// Path to input wikidata dump.
    pub wikidata_source_path: std::path::PathBuf,

    /// Path to the output product file.
    pub target_products_path: std::path::PathBuf,

    /// Path to the output organisations file.
    pub target_organisations_path: std::path::PathBuf,

    /// Data sources.
    pub sources: SourcesConfig,
}

impl CondensationConfig {
    /// Constructs a new `CondensationConfig`.
    pub fn new(args: &CondensationArgs) -> CondensationConfig {
        let input_cache = std::path::PathBuf::from(&args.input_cache);
        let output_data = std::path::PathBuf::from(&args.output_data);
        Self {
            wikidata_source_path: input_cache.join("wikidata.jsonl"),
            target_products_path: output_data.join("products.json"),
            target_organisations_path: output_data.join("organisations.json"),
            sources: SourcesConfig::new(&args.input_data, &args.input_cache),
        }
    }

    /// Checks validity of the configuration.
    pub fn check(&self) -> Result<(), CheckError> {
        utils::path_exists(&self.wikidata_source_path)?;
        utils::path_creatable(&self.target_products_path)?;
        utils::path_creatable(&self.target_organisations_path)?;
        self.sources.check()?;
        Ok(())
    }
}

/// Configuration for the `transcribe` command.
#[derive(Clone, Debug)]
pub struct TranscriptionConfig {
    /// Path to the output info file.
    pub sustainity_path: std::path::PathBuf,

    /// Path to the output info file.
    pub target_info_path: std::path::PathBuf,
}

impl TranscriptionConfig {
    //i/ Constructs a new `TranscriptionConfig`.
    pub fn new(args: &TranscriptionArgs) -> TranscriptionConfig {
        let input_data = std::path::PathBuf::from(&args.input_data);
        let output_data = std::path::PathBuf::from(&args.output_data);
        Self {
            sustainity_path: input_data.join("sustainity.yaml"),
            target_info_path: output_data.join("info.json"),
        }
    }

    /// Checks validity of the configuration.
    pub fn check(&self) -> Result<(), CheckError> {
        utils::path_exists(&self.sustainity_path)?;
        utils::path_creatable(&self.target_info_path)?;
        Ok(())
    }
}

/// Configuration for the `analyze` command.
#[derive(Clone, Debug)]
pub struct AnalysisConfig {
    /// Path to output Wikidata cache.
    pub wikidata_cache_path: std::path::PathBuf,

    /// Path to output filtered .
    pub wikidata_filtered_dump_path: std::path::PathBuf,
}

impl AnalysisConfig {
    /// Constructs a new `FilteringConfig`.
    pub fn new(args: &AnalysisArgs) -> AnalysisConfig {
        let input_cache = std::path::PathBuf::from(&args.input_cache);
        Self {
            wikidata_cache_path: input_cache.join("wikidata_cache.json"),
            wikidata_filtered_dump_path: input_cache.join("wikidata.jsonl"),
        }
    }

    /// Checks validity of the configuration.
    pub fn check(&self) -> Result<(), CheckError> {
        utils::path_exists(&self.wikidata_cache_path)?;
        utils::path_exists(&self.wikidata_filtered_dump_path)?;
        Ok(())
    }
}

impl AsRef<SourcesConfig> for CondensationConfig {
    fn as_ref(&self) -> &SourcesConfig {
        &self.sources
    }
}

impl AsRef<SourcesConfig> for FilteringConfig {
    fn as_ref(&self) -> &SourcesConfig {
        &self.sources
    }
}

impl AsRef<PrefilteringConfig> for PrefilteringConfig {
    fn as_ref(&self) -> &PrefilteringConfig {
        self
    }
}

impl AsRef<AnalysisConfig> for AnalysisConfig {
    fn as_ref(&self) -> &AnalysisConfig {
        self
    }
}

/// Configuration for the program.
#[derive(Debug, Clone)]
pub enum Config {
    Prefiltering(PrefilteringConfig),
    Filtering(FilteringConfig),
    Condensation(CondensationConfig),
    Transcription(TranscriptionConfig),
    Analysis(AnalysisConfig),
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
            Commands::Analyze(args) => Config::Analysis(AnalysisConfig::new(&args)),
        }
    }
}
