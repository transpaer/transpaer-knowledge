use clap::{Parser, Subcommand};

use crate::{errors::ConfigCheckError, utils};

/// Arguments of the `prefilter` command.
#[derive(Parser, Debug)]
pub struct PrefilteringArgs {
    /// Origin data directory.
    #[arg(long)]
    origin: String,

    /// Cache directory.
    #[arg(long)]
    cache: String,
}

/// Arguments of the `filter` command.
#[derive(Parser, Debug)]
pub struct FilteringArgs {
    /// Origin data directory.
    #[arg(long)]
    origin: String,

    /// Source data directory.
    #[arg(long)]
    source: String,

    /// Cache directory.
    #[arg(long)]
    cache: String,
}

/// Arguments of the `condense` command.
#[derive(Parser, Debug)]
pub struct CondensationArgs {
    /// Origin data directory.
    #[arg(long)]
    origin: String,

    /// Source data directory.
    #[arg(long)]
    source: String,

    /// Cache directory.
    #[arg(long)]
    cache: String,

    /// Target data directory.
    #[arg(long)]
    target: String,
}

/// Arguments of the `transcribe` command.
#[derive(Parser, Debug)]
pub struct TranscriptionArgs {
    /// Source data directory.
    #[arg(long)]
    source: String,

    /// Target data directory.
    #[arg(long)]
    target: String,
}

/// Arguments of the `analyse` command.
#[derive(Parser, Debug)]
pub struct AnalysisArgs {
    /// Cache directory.
    #[arg(long)]
    cache: String,
}

/// Arguments of the `connect` command.
#[derive(Parser, Debug)]
pub struct ConnectionArgs {
    #[arg(long)]
    wikidata_path: String,

    #[arg(long)]
    input_path: String,

    #[arg(long)]
    output_path: String,
}

/// All arguments of the program.
#[derive(Subcommand, Debug)]
pub enum Commands {
    Prefilter(PrefilteringArgs),
    Filter(FilteringArgs),
    Condense(CondensationArgs),
    Transcribe(TranscriptionArgs),
    Analyze(AnalysisArgs),
    Connect(ConnectionArgs),
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

    /// Path to original EU Ecolabel data.
    pub eu_ecolabel_original_path: std::path::PathBuf,

    /// Path to Wikidata ID map data for EU Ecolabel data.
    pub eu_ecolabel_match_path: std::path::PathBuf,

    /// Path to TCO data.
    pub tco_path: std::path::PathBuf,

    /// Path to Fashion Transparency Index data.
    pub fashion_transparency_index_path: std::path::PathBuf,
}

impl SourcesConfig {
    /// Constructs a new `SourceConfig`.
    pub fn new(origin: &str, source: &str, cache: &str) -> SourcesConfig {
        let source = std::path::PathBuf::from(source);
        let origin = std::path::PathBuf::from(origin);
        let cache = std::path::PathBuf::from(cache);
        Self {
            wikidata_cache_path: cache.join("wikidata_cache.json"),
            bcorp_path: origin.join("bcorp.csv"),
            eu_ecolabel_original_path: origin.join("eu_ecolabel_products.csv"),
            eu_ecolabel_match_path: source.join("eu_ecolabel.yaml"),
            tco_path: source.join("tco.yaml"),
            fashion_transparency_index_path: source.join("fashion_transparency_index.yaml"),
        }
    }

    /// Checks validity of the configuration.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
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
        let origin = std::path::PathBuf::from(&args.origin);
        let cache = std::path::PathBuf::from(&args.cache);
        Self {
            wikidata_dump_path: origin.join("wikidata-20230417-all.json.gz"),
            wikidata_cache_path: cache.join("wikidata_cache.json"),
        }
    }

    /// Checks validity of the configuration.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
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
        let origin = std::path::PathBuf::from(&args.origin);
        let cache = std::path::PathBuf::from(&args.cache);
        Self {
            wikidata_full_dump_path: origin.join("wikidata-20230417-all.json.gz"),
            wikidata_filtered_dump_path: cache.join("wikidata.jsonl"),
            sources: SourcesConfig::new(&args.origin, &args.source, &args.cache),
        }
    }

    /// Checks validity of the configuration.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
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
        let cache = std::path::PathBuf::from(&args.cache);
        let target = std::path::PathBuf::from(&args.target);
        Self {
            wikidata_source_path: cache.join("wikidata.jsonl"),
            target_products_path: target.join("products.json"),
            target_organisations_path: target.join("organisations.json"),
            sources: SourcesConfig::new(&args.origin, &args.source, &args.cache),
        }
    }

    /// Checks validity of the configuration.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
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
    /// Path to the input library file.
    pub library_source_path: std::path::PathBuf,

    /// Path to the output library file.
    pub library_target_path: std::path::PathBuf,
}

impl TranscriptionConfig {
    //i/ Constructs a new `TranscriptionConfig`.
    pub fn new(args: &TranscriptionArgs) -> TranscriptionConfig {
        let source = std::path::PathBuf::from(&args.source);
        let target = std::path::PathBuf::from(&args.target);
        Self {
            library_source_path: source.join("sustainity_library.yaml"),
            library_target_path: target.join("library.json"),
        }
    }

    /// Checks validity of the configuration.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        utils::path_exists(&self.library_source_path)?;
        utils::path_creatable(&self.library_target_path)?;
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
        let cache = std::path::PathBuf::from(&args.cache);
        Self {
            wikidata_cache_path: cache.join("wikidata_cache.json"),
            wikidata_filtered_dump_path: cache.join("wikidata.jsonl"),
        }
    }

    /// Checks validity of the configuration.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        utils::path_exists(&self.wikidata_cache_path)?;
        utils::path_exists(&self.wikidata_filtered_dump_path)?;
        Ok(())
    }
}

/// Configuration for the `connect` command.
#[derive(Clone, Debug)]
pub struct ConnectionConfig {
    /// Path to Wikidata data.
    pub wikidata_path: std::path::PathBuf,

    /// Path to input data file.
    pub input_path: std::path::PathBuf,

    /// Path to output mapping file.
    pub output_path: std::path::PathBuf,
}

impl ConnectionConfig {
    /// Constructs a new `ConnectionConfig`.
    pub fn new(args: &ConnectionArgs) -> ConnectionConfig {
        Self {
            wikidata_path: std::path::PathBuf::from(&args.wikidata_path),
            input_path: std::path::PathBuf::from(&args.input_path),
            output_path: std::path::PathBuf::from(&args.output_path),
        }
    }

    /// Checks validity of the configuration.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        utils::path_exists(&self.wikidata_path)?;
        utils::path_exists(&self.input_path)?;
        utils::path_creatable(&self.output_path)?;
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

impl AsRef<ConnectionConfig> for ConnectionConfig {
    fn as_ref(&self) -> &ConnectionConfig {
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
    Connection(ConnectionConfig),
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
            Commands::Connect(args) => Config::Connection(ConnectionConfig::new(&args)),
        }
    }
}
