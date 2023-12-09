use clap::Parser;

use crate::{commands, errors::ConfigCheckError, utils};

/// Configuration for `WikidataGather`.
#[must_use]
#[derive(Debug, Clone)]
pub struct WikidataGathererConfig {
    /// Path to Wikidata data.
    pub wikidata_path: std::path::PathBuf,
}

impl WikidataGathererConfig {
    /// Constructs a new `WikidataGathererConfig` with filteresd Wikidata dump.
    pub fn new_filtered(cache: &str) -> WikidataGathererConfig {
        let cache = std::path::PathBuf::from(&cache);
        Self { wikidata_path: cache.join("wikidata.jsonl") }
    }

    /// Constructs a new `WikidataGathererConfig` with full Wikidata dump.
    pub fn new_full(origin: &str) -> WikidataGathererConfig {
        let origin = std::path::PathBuf::from(&origin);
        Self { wikidata_path: origin.join("wikidata-20231120-all.json.gz") }
    }

    /// Constructs a new `WikidataGathererConfig`.
    pub fn new_with_path(path: &str) -> WikidataGathererConfig {
        let wikidata_path = std::path::PathBuf::from(&path);
        Self { wikidata_path }
    }

    /// Checks validity of the configuration.
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        utils::path_exists(&self.wikidata_path)?;
        Ok(())
    }
}

/// Configuration for `WikidataGatherer`.
#[must_use]
#[derive(Debug, Clone)]
pub struct OpenFoodFactsGathererConfig {
    /// Path to Open Food Facts data.
    pub open_food_facts_path: std::path::PathBuf,
}

impl OpenFoodFactsGathererConfig {
    pub fn new(origin: &str) -> Self {
        let origin = std::path::PathBuf::from(origin);
        Self { open_food_facts_path: origin.join("en.openfoodfacts.org.products.csv") }
    }

    /// Checks validity of the configuration.
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        utils::path_exists(&self.open_food_facts_path)?;
        Ok(())
    }
}

/// Configuration for `WikidataGatherer`.
#[must_use]
#[derive(Debug, Clone)]
pub struct EuEcolabelGathererConfig {
    /// Path to Open Food Facts data.
    pub eu_ecolabel_path: std::path::PathBuf,
}

impl EuEcolabelGathererConfig {
    pub fn new(origin: &str) -> Self {
        let origin = std::path::PathBuf::from(origin);
        Self { eu_ecolabel_path: origin.join("eu_ecolabel_products.csv") }
    }

    /// Checks validity of the configuration.
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        utils::path_exists(&self.eu_ecolabel_path)?;
        Ok(())
    }
}

/// Configuration for `FullGatherer`.
#[must_use]
#[derive(Debug, Clone)]
pub struct FullGathererConfig {
    /// Wikidata gatherer config.
    pub wiki: WikidataGathererConfig,

    /// Open Food Facts gatherer config.
    pub off: OpenFoodFactsGathererConfig,

    /// EU Ecolabel gatherer config.
    pub eu_ecolabel: EuEcolabelGathererConfig,
}

impl FullGathererConfig {
    /// Constructs a new `WikidataGathererConfig`.
    pub fn new(origin: &str, cache: &str) -> FullGathererConfig {
        Self {
            wiki: WikidataGathererConfig::new_filtered(cache),
            off: OpenFoodFactsGathererConfig::new(origin),
            eu_ecolabel: EuEcolabelGathererConfig::new(origin),
        }
    }

    /// Checks validity of the configuration.
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        self.wiki.check()?;
        self.off.check()?;
        self.eu_ecolabel.check()?;
        Ok(())
    }
}

/// Subconfiguration related to source files used by several other configs.
#[must_use]
#[derive(Debug, Clone)]
pub struct SourcesConfig {
    /// Path to input Wikidata cache.
    pub wikidata_cache_path: std::path::PathBuf,

    /// Path to BCorp data.
    pub bcorp_path: std::path::PathBuf,

    /// Path to original EU Ecolabel data.
    pub eu_ecolabel_original_path: std::path::PathBuf,

    /// Path to mapping from names to Wikidata IDs.
    pub match_path: std::path::PathBuf,

    /// Path to TCO data.
    pub tco_path: std::path::PathBuf,

    /// Path to Fashion Transparency Index data.
    pub fashion_transparency_index_path: std::path::PathBuf,

    /// Path to file mapping Open Food Facts sell countries to Sustainity regions.
    pub open_food_facts_countries_path: std::path::PathBuf,
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
            match_path: source.join("matches.yaml"),
            tco_path: source.join("tco.yaml"),
            fashion_transparency_index_path: source.join("fashion_transparency_index.yaml"),
            open_food_facts_countries_path: source.join("open_food_facts_countries.yaml"),
        }
    }

    /// Checks validity of the configuration.
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        utils::path_exists(&self.wikidata_cache_path)?;
        utils::path_exists(&self.bcorp_path)?;
        utils::path_exists(&self.eu_ecolabel_original_path)?;
        utils::path_exists(&self.match_path)?;
        utils::path_exists(&self.tco_path)?;
        utils::path_exists(&self.fashion_transparency_index_path)?;
        utils::path_exists(&self.open_food_facts_countries_path)?;
        Ok(())
    }
}

/// Configuration for the `filter-products` command.
#[must_use]
#[derive(Debug, Clone)]
pub struct PrefilteringConfig {
    /// Path to output Wikidata cache.
    pub wikidata_cache_path: std::path::PathBuf,

    /// `WikidataGatherer` config.
    pub wikidata_gatherer: WikidataGathererConfig,
}

impl PrefilteringConfig {
    /// Constructs a new `Prefiltering`.
    pub fn new(args: &commands::PrefilteringArgs) -> PrefilteringConfig {
        let cache = std::path::PathBuf::from(&args.cache);
        Self {
            wikidata_cache_path: cache.join("wikidata_cache.json"),
            wikidata_gatherer: WikidataGathererConfig::new_full(&args.origin),
        }
    }

    /// Checks validity of the configuration.
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        utils::path_creatable(&self.wikidata_cache_path)?;
        self.wikidata_gatherer.check()?;
        Ok(())
    }
}

/// Configuration for the `filter-manufacturers` command.
#[must_use]
#[derive(Debug, Clone)]
pub struct FilteringConfig {
    /// Path to output filtered .
    pub wikidata_filtered_dump_path: std::path::PathBuf,

    /// Data sources.
    pub sources: SourcesConfig,

    /// `WikidataGatherer` config.
    pub wikidata_gatherer: WikidataGathererConfig,
}

impl FilteringConfig {
    /// Constructs a new `FilteringConfig`.
    pub fn new(args: &commands::FilteringArgs) -> FilteringConfig {
        let cache = std::path::PathBuf::from(&args.cache);
        Self {
            wikidata_filtered_dump_path: cache.join("wikidata.jsonl"),
            sources: SourcesConfig::new(&args.origin, &args.source, &args.cache),
            wikidata_gatherer: WikidataGathererConfig::new_full(&args.origin),
        }
    }

    /// Checks validity of the configuration.
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        utils::path_creatable(&self.wikidata_filtered_dump_path)?;
        self.sources.check()?;
        self.wikidata_gatherer.check()?;
        Ok(())
    }
}

#[must_use]
#[derive(Clone, Debug)]
pub struct UpdatingConfig {
    pub off: OpenFoodFactsGathererConfig,
    pub sources: SourcesConfig,
}

impl UpdatingConfig {
    /// Constructs a new `UpdatingConfig`.
    pub fn new(args: &commands::UpdatingArgs) -> UpdatingConfig {
        Self {
            off: OpenFoodFactsGathererConfig::new(&args.origin),
            sources: SourcesConfig::new(&args.origin, &args.source, &args.cache),
        }
    }

    /// Checks validity of the configuration.
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        self.off.check()?;
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
#[must_use]
#[derive(Debug, Clone)]
pub struct CondensationConfig {
    /// Output file paths.
    pub target: Box<CondensationInnerConfig>,

    /// Data sources.
    pub sources: SourcesConfig,

    /// `FullGatherer` config.
    pub full_gatherer: FullGathererConfig,
}

impl CondensationConfig {
    /// Constructs a new `CondensationConfig`.
    pub fn new(args: &commands::CondensationArgs) -> CondensationConfig {
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
            full_gatherer: FullGathererConfig::new(&args.origin, &args.cache),
        }
    }

    /// Checks validity of the configuration.
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
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
        self.full_gatherer.check()?;
        Ok(())
    }
}

/// Configuration for the `transcribe` command.
#[must_use]
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
    pub fn new(args: &commands::TranscriptionArgs) -> TranscriptionConfig {
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
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        utils::path_exists(&self.library_file_path)?;
        utils::dir_exists(&self.library_dir_path)?;
        utils::path_creatable(&self.library_target_path)?;
        Ok(())
    }
}

/// Configuration for the `analyze` command.
#[must_use]
#[derive(Clone, Debug)]
pub struct AnalysisConfig {
    /// Path to output Wikidata cache.
    pub wikidata_cache_path: std::path::PathBuf,

    /// `Wikidatagatherer` config.
    pub wikidata_gatherer: WikidataGathererConfig,
}

impl AnalysisConfig {
    /// Constructs a new `FilteringConfig`.
    pub fn new(args: &commands::AnalysisArgs) -> AnalysisConfig {
        let cache = std::path::PathBuf::from(&args.cache);
        Self {
            wikidata_cache_path: cache.join("wikidata_cache.json"),
            wikidata_gatherer: WikidataGathererConfig::new_filtered(&args.cache),
        }
    }

    /// Checks validity of the configuration.
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        utils::path_exists(&self.wikidata_cache_path)?;
        self.wikidata_gatherer.check()?;
        Ok(())
    }
}

/// Configuration for the `connect` command.
#[must_use]
#[derive(Clone, Debug)]
pub struct ConnectionConfig {
    /// Path to input EU Ecolabel data file.
    pub eu_ecolabel_input_path: std::path::PathBuf,

    /// Path to input Open Food Facts data file.
    pub open_food_facts_input_path: std::path::PathBuf,

    /// Path to output data file.
    pub output_path: std::path::PathBuf,

    /// `WikidataGatherer` config.
    pub wikidata_gatherer: WikidataGathererConfig,
}

impl ConnectionConfig {
    /// Constructs a new `ConnectionConfig`.
    pub fn new(args: &commands::ConnectionArgs) -> ConnectionConfig {
        let origin = std::path::PathBuf::from(&args.origin);
        let source = std::path::PathBuf::from(&args.source);
        Self {
            eu_ecolabel_input_path: origin.join("eu_ecolabel_products.csv"),
            open_food_facts_input_path: origin.join("en.openfoodfacts.org.products.csv"),
            output_path: source.join("matches.yaml"),
            wikidata_gatherer: WikidataGathererConfig::new_with_path(&args.wikidata_path),
        }
    }

    /// Checks validity of the configuration.
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        utils::path_exists(&self.eu_ecolabel_input_path)?;
        utils::path_exists(&self.open_food_facts_input_path)?;
        utils::path_creatable(&self.output_path)?;
        self.wikidata_gatherer.check()?;
        Ok(())
    }
}

impl From<&FullGathererConfig> for WikidataGathererConfig {
    fn from(config: &FullGathererConfig) -> WikidataGathererConfig {
        config.wiki.clone()
    }
}

impl From<&FullGathererConfig> for OpenFoodFactsGathererConfig {
    fn from(config: &FullGathererConfig) -> OpenFoodFactsGathererConfig {
        config.off.clone()
    }
}

impl From<&FullGathererConfig> for EuEcolabelGathererConfig {
    fn from(config: &FullGathererConfig) -> EuEcolabelGathererConfig {
        config.eu_ecolabel.clone()
    }
}

impl From<&CondensationConfig> for FullGathererConfig {
    fn from(config: &CondensationConfig) -> FullGathererConfig {
        config.full_gatherer.clone()
    }
}

impl From<&CondensationConfig> for WikidataGathererConfig {
    fn from(config: &CondensationConfig) -> WikidataGathererConfig {
        config.full_gatherer.wiki.clone()
    }
}

impl From<&CondensationConfig> for OpenFoodFactsGathererConfig {
    fn from(config: &CondensationConfig) -> OpenFoodFactsGathererConfig {
        config.full_gatherer.off.clone()
    }
}

impl From<&UpdatingConfig> for OpenFoodFactsGathererConfig {
    fn from(config: &UpdatingConfig) -> OpenFoodFactsGathererConfig {
        config.off.clone()
    }
}

impl From<&CondensationConfig> for EuEcolabelGathererConfig {
    fn from(config: &CondensationConfig) -> EuEcolabelGathererConfig {
        config.full_gatherer.eu_ecolabel.clone()
    }
}

impl From<&FilteringConfig> for WikidataGathererConfig {
    fn from(config: &FilteringConfig) -> WikidataGathererConfig {
        config.wikidata_gatherer.clone()
    }
}

impl From<&PrefilteringConfig> for WikidataGathererConfig {
    fn from(config: &PrefilteringConfig) -> WikidataGathererConfig {
        config.wikidata_gatherer.clone()
    }
}

impl From<&AnalysisConfig> for WikidataGathererConfig {
    fn from(config: &AnalysisConfig) -> WikidataGathererConfig {
        config.wikidata_gatherer.clone()
    }
}

impl From<&ConnectionConfig> for WikidataGathererConfig {
    fn from(config: &ConnectionConfig) -> WikidataGathererConfig {
        config.wikidata_gatherer.clone()
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

impl From<&UpdatingConfig> for SourcesConfig {
    fn from(config: &UpdatingConfig) -> SourcesConfig {
        config.sources.clone()
    }
}

/// Configuration for the program.
#[must_use]
#[derive(Debug, Clone)]
pub enum Config {
    Prefiltering(PrefilteringConfig),
    Filtering(FilteringConfig),
    Updating(UpdatingConfig),
    Condensation(CondensationConfig),
    Transcription(TranscriptionConfig),
    Analysis(AnalysisConfig),
    Connection(ConnectionConfig),
}

impl Config {
    /// Constructs a new config from `Args::parse()`.
    pub fn new_from_args() -> Config {
        use commands::{Args, Commands};

        let args = Args::parse();
        match args.command {
            Commands::Prefilter(args) => Config::Prefiltering(PrefilteringConfig::new(&args)),
            Commands::Filter(args) => Config::Filtering(FilteringConfig::new(&args)),
            Commands::Update(args) => Config::Updating(UpdatingConfig::new(&args)),
            Commands::Condense(args) => Config::Condensation(CondensationConfig::new(&args)),
            Commands::Transcribe(args) => Config::Transcription(TranscriptionConfig::new(&args)),
            Commands::Analyze(args) => Config::Analysis(AnalysisConfig::new(&args)),
            Commands::Connect(args) => Config::Connection(ConnectionConfig::new(&args)),
        }
    }
}
