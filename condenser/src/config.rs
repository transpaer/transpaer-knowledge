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

    /// Library data directory.
    #[arg(long)]
    library: String,

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

/// Configuration for `WikidataRunner`.
#[derive(Debug, Clone)]
pub struct WikidataRunnerConfig {
    /// Path to Wikidata data.
    pub wikidata_path: std::path::PathBuf,
}

impl WikidataRunnerConfig {
    /// Constructs a new `WikidataRunnerConfig` with filteresd Wikidata dump.
    pub fn new_filtered(cache: &str) -> WikidataRunnerConfig {
        let cache = std::path::PathBuf::from(&cache);
        Self { wikidata_path: cache.join("wikidata.jsonl") }
    }

    /// Constructs a new `WikidataRunnerConfig` with full Wikidata dump.
    pub fn new_full(origin: &str) -> WikidataRunnerConfig {
        let origin = std::path::PathBuf::from(&origin);
        Self { wikidata_path: origin.join("wikidata-20230417-all.json.gz") }
    }

    /// Constructs a new `WikidataRunnerConfig`.
    pub fn new_with_path(path: &str) -> WikidataRunnerConfig {
        let wikidata_path = std::path::PathBuf::from(&path);
        Self { wikidata_path }
    }

    /// Checks validity of the configuration.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        utils::path_exists(&self.wikidata_path)?;
        Ok(())
    }
}

/// Configuration for `WikidataRunner`.
#[derive(Debug, Clone)]
pub struct FullRunnerConfig {
    /// Path to Wikidata data.
    pub wikidata_path: std::path::PathBuf,

    /// Path to Open Food Facts data.
    pub open_food_facts_path: std::path::PathBuf,

    /// Path to EU Ecolabel data.
    pub eu_ecolabel_path: std::path::PathBuf,
}

impl FullRunnerConfig {
    /// Constructs a new `WikidataRunnerConfig`.
    pub fn new(origin: &str, cache: &str) -> FullRunnerConfig {
        let origin = std::path::PathBuf::from(origin);
        let cache = std::path::PathBuf::from(cache);
        Self {
            wikidata_path: cache.join("wikidata.jsonl"),
            open_food_facts_path: origin.join("en.openfoodfacts.org.products.csv"),
            eu_ecolabel_path: origin.join("eu_ecolabel_products.csv"),
        }
    }

    /// Checks validity of the configuration.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        utils::path_exists(&self.wikidata_path)?;
        utils::path_exists(&self.open_food_facts_path)?;
        utils::path_exists(&self.eu_ecolabel_path)?;
        Ok(())
    }
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
    /// Path to output Wikidata cache.
    pub wikidata_cache_path: std::path::PathBuf,

    /// `WikidataRunner` config.
    pub wikidata_runner: WikidataRunnerConfig,
}

impl PrefilteringConfig {
    /// Constructs a new `Prefiltering`.
    pub fn new(args: &PrefilteringArgs) -> PrefilteringConfig {
        let cache = std::path::PathBuf::from(&args.cache);
        Self {
            wikidata_cache_path: cache.join("wikidata_cache.json"),
            wikidata_runner: WikidataRunnerConfig::new_full(&args.origin),
        }
    }

    /// Checks validity of the configuration.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        utils::path_creatable(&self.wikidata_cache_path)?;
        self.wikidata_runner.check()?;
        Ok(())
    }
}

/// Configuration for the `filter-manufacturers` command.
#[derive(Debug, Clone)]
pub struct FilteringConfig {
    /// Path to output filtered .
    pub wikidata_filtered_dump_path: std::path::PathBuf,

    /// Data sources.
    pub sources: SourcesConfig,

    /// `WikidataRunner` config.
    pub wikidata_runner: WikidataRunnerConfig,
}

impl FilteringConfig {
    /// Constructs a new `FilteringConfig`.
    pub fn new(args: &FilteringArgs) -> FilteringConfig {
        let cache = std::path::PathBuf::from(&args.cache);
        Self {
            wikidata_filtered_dump_path: cache.join("wikidata.jsonl"),
            sources: SourcesConfig::new(&args.origin, &args.source, &args.cache),
            wikidata_runner: WikidataRunnerConfig::new_full(&args.cache),
        }
    }

    /// Checks validity of the configuration.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        utils::path_creatable(&self.wikidata_filtered_dump_path)?;
        self.sources.check()?;
        self.wikidata_runner.check()?;
        Ok(())
    }
}

/// Part of the configuration for the `condense` command.
///
/// Extracted to keep size of all configuration structures similar.
#[derive(Debug, Clone)]
pub struct CondensationInnerConfig {
    pub organisations_path: std::path::PathBuf,
    pub organisation_keywords_path: std::path::PathBuf,
    pub organisation_keyword_edges_path: std::path::PathBuf,
    pub products_path: std::path::PathBuf,
    pub product_keywords_path: std::path::PathBuf,
    pub product_keyword_edges_path: std::path::PathBuf,
    pub gtins_path: std::path::PathBuf,
    pub gtin_edges_path: std::path::PathBuf,
    pub categories_path: std::path::PathBuf,
    pub category_edges_path: std::path::PathBuf,
    pub manufacturing_edges_path: std::path::PathBuf,
    pub presentations_path: std::path::PathBuf,
}

/// Configuration for the `condense` command.
#[derive(Debug, Clone)]
pub struct CondensationConfig {
    /// Output file paths.
    pub target: Box<CondensationInnerConfig>,

    /// Data sources.
    pub sources: SourcesConfig,

    /// `FullRunner` config.
    pub full_runner: FullRunnerConfig,
}

impl CondensationConfig {
    /// Constructs a new `CondensationConfig`.
    pub fn new(args: &CondensationArgs) -> CondensationConfig {
        let target = std::path::PathBuf::from(&args.target);
        Self {
            target: Box::new(CondensationInnerConfig {
                organisations_path: target.join("organisations.jsonl"),
                organisation_keywords_path: target.join("organisation_keywords.jsonl"),
                organisation_keyword_edges_path: target.join("organisation_keyword_edges.jsonl"),
                products_path: target.join("products.jsonl"),
                product_keywords_path: target.join("product_keywords.jsonl"),
                product_keyword_edges_path: target.join("product_keyword_edges.jsonl"),
                gtins_path: target.join("gtins.jsonl"),
                gtin_edges_path: target.join("gtin_edges.jsonl"),
                categories_path: target.join("categories.jsonl"),
                category_edges_path: target.join("category_edges.jsonl"),
                manufacturing_edges_path: target.join("manufacturing_edges.jsonl"),
                presentations_path: target.join("presentations.jsonl"),
            }),
            sources: SourcesConfig::new(&args.origin, &args.source, &args.cache),
            full_runner: FullRunnerConfig::new(&args.origin, &args.cache),
        }
    }

    /// Checks validity of the configuration.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        utils::path_creatable(&self.target.organisations_path)?;
        utils::path_creatable(&self.target.organisation_keywords_path)?;
        utils::path_creatable(&self.target.organisation_keyword_edges_path)?;
        utils::path_creatable(&self.target.products_path)?;
        utils::path_creatable(&self.target.product_keywords_path)?;
        utils::path_creatable(&self.target.product_keyword_edges_path)?;
        utils::path_creatable(&self.target.gtins_path)?;
        utils::path_creatable(&self.target.gtin_edges_path)?;
        utils::path_creatable(&self.target.categories_path)?;
        utils::path_creatable(&self.target.category_edges_path)?;
        utils::path_creatable(&self.target.manufacturing_edges_path)?;
        utils::path_creatable(&self.target.presentations_path)?;
        self.sources.check()?;
        self.full_runner.check()?;
        Ok(())
    }
}

/// Configuration for the `transcribe` command.
#[derive(Clone, Debug)]
pub struct TranscriptionConfig {
    /// Path to the input library file.
    pub library_file_path: std::path::PathBuf,

    /// Path to the input library directory.
    pub library_dir_path: std::path::PathBuf,

    /// Path to the output library file.
    pub library_target_path: std::path::PathBuf,
}

impl TranscriptionConfig {
    //i/ Constructs a new `TranscriptionConfig`.
    pub fn new(args: &TranscriptionArgs) -> TranscriptionConfig {
        let source = std::path::PathBuf::from(&args.source);
        let library = std::path::PathBuf::from(&args.library);
        let target = std::path::PathBuf::from(&args.target);
        Self {
            library_file_path: source.join("sustainity_library.yaml"),
            library_dir_path: library,
            library_target_path: target.join("library.jsonl"),
        }
    }

    /// Checks validity of the configuration.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        utils::path_exists(&self.library_file_path)?;
        utils::dir_exists(&self.library_dir_path)?;
        utils::path_creatable(&self.library_target_path)?;
        Ok(())
    }
}

/// Configuration for the `analyze` command.
#[derive(Clone, Debug)]
pub struct AnalysisConfig {
    /// Path to output Wikidata cache.
    pub wikidata_cache_path: std::path::PathBuf,

    /// `WikidataRunner` config.
    pub wikidata_runner: WikidataRunnerConfig,
}

impl AnalysisConfig {
    /// Constructs a new `FilteringConfig`.
    pub fn new(args: &AnalysisArgs) -> AnalysisConfig {
        let cache = std::path::PathBuf::from(&args.cache);
        Self {
            wikidata_cache_path: cache.join("wikidata_cache.json"),
            wikidata_runner: WikidataRunnerConfig::new_filtered(&args.cache),
        }
    }

    /// Checks validity of the configuration.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        utils::path_exists(&self.wikidata_cache_path)?;
        self.wikidata_runner.check()?;
        Ok(())
    }
}

/// Configuration for the `connect` command.
#[derive(Clone, Debug)]
pub struct ConnectionConfig {
    /// Path to input data file.
    pub input_path: std::path::PathBuf,

    /// Path to output mapping file.
    pub output_path: std::path::PathBuf,

    /// `WikidataRunner` config.
    pub wikidata_runner: WikidataRunnerConfig,
}

impl ConnectionConfig {
    /// Constructs a new `ConnectionConfig`.
    pub fn new(args: &ConnectionArgs) -> ConnectionConfig {
        Self {
            input_path: std::path::PathBuf::from(&args.input_path),
            output_path: std::path::PathBuf::from(&args.output_path),
            wikidata_runner: WikidataRunnerConfig::new_with_path(&args.wikidata_path),
        }
    }

    /// Checks validity of the configuration.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        utils::path_exists(&self.input_path)?;
        utils::path_creatable(&self.output_path)?;
        self.wikidata_runner.check()?;
        Ok(())
    }
}

impl From<&CondensationConfig> for FullRunnerConfig {
    fn from(config: &CondensationConfig) -> FullRunnerConfig {
        config.full_runner.clone()
    }
}

impl From<&FilteringConfig> for WikidataRunnerConfig {
    fn from(config: &FilteringConfig) -> WikidataRunnerConfig {
        config.wikidata_runner.clone()
    }
}

impl From<&PrefilteringConfig> for WikidataRunnerConfig {
    fn from(config: &PrefilteringConfig) -> WikidataRunnerConfig {
        config.wikidata_runner.clone()
    }
}

impl From<&AnalysisConfig> for WikidataRunnerConfig {
    fn from(config: &AnalysisConfig) -> WikidataRunnerConfig {
        config.wikidata_runner.clone()
    }
}

impl From<&ConnectionConfig> for WikidataRunnerConfig {
    fn from(config: &ConnectionConfig) -> WikidataRunnerConfig {
        config.wikidata_runner.clone()
    }
}

impl From<&CondensationConfig> for SourcesConfig {
    fn from(config: &CondensationConfig) -> SourcesConfig {
        config.sources.clone()
    }
}

impl From<&FilteringConfig> for SourcesConfig {
    fn from(config: &FilteringConfig) -> SourcesConfig {
        config.sources.clone()
    }
}

impl From<&PrefilteringConfig> for PrefilteringConfig {
    fn from(config: &PrefilteringConfig) -> PrefilteringConfig {
        config.clone()
    }
}

impl From<&AnalysisConfig> for AnalysisConfig {
    fn from(config: &AnalysisConfig) -> AnalysisConfig {
        config.clone()
    }
}

impl From<&ConnectionConfig> for ConnectionConfig {
    fn from(config: &ConnectionConfig) -> ConnectionConfig {
        config.clone()
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
