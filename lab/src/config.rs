use std::path::PathBuf;

use clap::Parser;

use crate::{commands, errors::ConfigCheckError, utils};

pub use commands::CondensationGroup;

/// Configuration for `WikidataGather`.
#[must_use]
#[derive(Debug, Clone)]
pub struct WikidataProducerConfig {
    /// Path to Wikidata data.
    pub wikidata_path: PathBuf,
}

impl WikidataProducerConfig {
    /// Constructs a new `WikidataProducerConfig` with filteresd Wikidata dump.
    pub fn new_filtered(cache: &str) -> WikidataProducerConfig {
        let cache = PathBuf::from(&cache);
        Self { wikidata_path: cache.join("wikidata.jsonl") }
    }

    /// Constructs a new `WikidataProducerConfig` with full Wikidata dump.
    pub fn new_full(origin: &str) -> WikidataProducerConfig {
        let origin = PathBuf::from(&origin);
        Self { wikidata_path: origin.join("wikidata-20250519-all.json.gz") }
    }

    /// Constructs a new `WikidataProducerConfig`.
    pub fn new_with_path(path: &str) -> WikidataProducerConfig {
        let wikidata_path = PathBuf::from(&path);
        Self { wikidata_path }
    }

    /// Checks validity of the configuration.
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        utils::file_exists(&self.wikidata_path)?;
        Ok(())
    }
}

/// Configuration for `WikidataGatherer`.
#[must_use]
#[derive(Debug, Clone)]
pub struct OpenFoodFactsProducerConfig {
    /// Path to Open Food Facts data.
    pub open_food_facts_path: PathBuf,
}

impl OpenFoodFactsProducerConfig {
    pub fn new(origin: &str) -> Self {
        let origin = PathBuf::from(origin);
        Self { open_food_facts_path: origin.join("en.openfoodfacts.org.products.csv") }
    }

    /// Checks validity of the configuration.
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        utils::file_exists(&self.open_food_facts_path)?;
        Ok(())
    }
}

/// Configuration for `WikidataGatherer`.
#[must_use]
#[derive(Debug, Clone)]
pub struct EuEcolabelProducerConfig {
    /// Path to Open Food Facts data.
    pub eu_ecolabel_path: PathBuf,
}

impl EuEcolabelProducerConfig {
    pub fn new(origin: &str) -> Self {
        let origin = PathBuf::from(origin);
        Self { eu_ecolabel_path: origin.join("eu_ecolabel_products.csv") }
    }

    /// Checks validity of the configuration.
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        utils::file_exists(&self.eu_ecolabel_path)?;
        Ok(())
    }
}

/// Configuration for `FullGatherer`.
#[must_use]
#[derive(Debug, Clone)]
pub struct FullProducerConfig {
    /// Wikidata gatherer config.
    pub wiki: WikidataProducerConfig,

    /// Open Food Facts gatherer config.
    pub off: OpenFoodFactsProducerConfig,

    /// EU Ecolabel gatherer config.
    pub eu_ecolabel: EuEcolabelProducerConfig,
}

impl FullProducerConfig {
    /// Constructs a new `WikidataProducerConfig`.
    pub fn new(origin: &str, cache: &str) -> FullProducerConfig {
        Self {
            wiki: WikidataProducerConfig::new_filtered(cache),
            off: OpenFoodFactsProducerConfig::new(origin),
            eu_ecolabel: EuEcolabelProducerConfig::new(origin),
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

/// Subconfiguration related to origin files used by several other configs.
#[allow(clippy::struct_field_names)]
#[must_use]
#[derive(Debug, Clone)]
pub struct OriginConfig {
    /// Path to original B-Corp data.
    pub bcorp_path: PathBuf,

    /// Path to original EU Ecolabel data.
    pub eu_ecolabel_path: PathBuf,
}

impl OriginConfig {
    /// Constructs a new `OriginConfig`.
    pub fn new(origin: &str) -> Self {
        let origin = PathBuf::from(origin);
        Self {
            bcorp_path: origin.join("bcorp.csv"),
            eu_ecolabel_path: origin.join("eu_ecolabel_products.csv"),
        }
    }

    /// Checks validity of the configuration.
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        utils::file_exists(&self.bcorp_path)?;
        utils::file_exists(&self.eu_ecolabel_path)?;
        Ok(())
    }
}

/// Subconfiguration related to support files used by several other configs.
#[allow(clippy::struct_field_names)]
#[must_use]
#[derive(Debug, Clone)]
pub struct SupportConfig {
    /// Path to TCO data.
    pub tco_path: PathBuf,

    /// Path to Fashion Transparency Index data.
    pub fashion_transparency_index_path: PathBuf,
}

impl SupportConfig {
    /// Constructs a new `SupportConfig`.
    pub fn new(support: &str) -> Self {
        let support = PathBuf::from(support);
        Self {
            tco_path: support.join("tco.yaml"),
            fashion_transparency_index_path: support.join("fashion_transparency_index.yaml"),
        }
    }

    /// Checks validity of the configuration.
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        utils::file_exists(&self.tco_path)?;
        utils::file_exists(&self.fashion_transparency_index_path)?;
        Ok(())
    }
}

/// Subconfiguration related to meta files used by several other configs.
#[allow(clippy::struct_field_names)]
#[must_use]
#[derive(Debug, Clone)]
pub struct MetaConfig {
    /// Path to file mapping Wikidata countries to Transpaer regions.
    pub wikidata_regions_path: PathBuf,

    /// Path to file mapping Wikidata classes to Transpaer categories.
    pub wikidata_categories_path: PathBuf,

    /// Path to file mapping Open Food Facts sell countries to Transpaer regions.
    pub open_food_facts_regions_path: PathBuf,

    /// Path to file mapping Open Food Facts categories to Transpaer categories.
    pub open_food_facts_categories_path: PathBuf,

    /// Path to file mapping B-Corp countries to Transpaer regions.
    pub bcorp_regions_path: PathBuf,

    /// Path to mapping from names to Wikidata IDs.
    pub match_path: PathBuf,
}

impl MetaConfig {
    /// Constructs a new `MetaConfig`.
    pub fn new(meta: &str) -> Self {
        let meta = PathBuf::from(meta);
        Self {
            wikidata_regions_path: meta.join("wikidata_regions.yaml"),
            wikidata_categories_path: meta.join("wikidata_categories.yaml"),
            open_food_facts_regions_path: meta.join("open_food_facts_regions.yaml"),
            open_food_facts_categories_path: meta.join("open_food_facts_categories.yaml"),
            bcorp_regions_path: meta.join("bcorp_regions.yaml"),
            match_path: meta.join("matches.yaml"),
        }
    }

    /// Checks validity of the configuration.
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        utils::file_exists(&self.wikidata_regions_path)?;
        utils::file_exists(&self.open_food_facts_regions_path)?;
        utils::file_exists(&self.bcorp_regions_path)?;
        utils::file_exists(&self.match_path)?;
        Ok(())
    }
}

/// Subconfiguration related to cache files used by several other configs.
#[allow(clippy::struct_field_names)]
#[must_use]
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Path to the cache wikidata path.
    pub wikidata_cache_path: PathBuf,
}

impl CacheConfig {
    /// Constructs a new `CacheConfig`.
    pub fn new(cache: &str) -> Self {
        let cache = PathBuf::from(cache);
        Self { wikidata_cache_path: cache.join("wikidata_cache.json") }
    }

    /// Checks validity of the configuration for reading.
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
    pub fn check_read(&self) -> Result<(), ConfigCheckError> {
        utils::file_exists(&self.wikidata_cache_path)?;
        Ok(())
    }

    /// Checks validity of the configuration for writing.
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
    pub fn check_write(&self) -> Result<(), ConfigCheckError> {
        utils::parent_creatable(&self.wikidata_cache_path)?;
        Ok(())
    }
}

/// Subconfiguration related to substrate files used by several other configs.
#[must_use]
#[derive(Debug, Clone)]
pub struct SubstrateConfig {
    /// Path to the substrate file directory.
    pub substrate_path: PathBuf,
}

impl SubstrateConfig {
    /// Constructs a new `SubstrateConfig`.
    pub fn new(substrate: &str) -> Self {
        let substrate_path = PathBuf::from(substrate);
        Self { substrate_path }
    }

    /// Checks validity of the configuration for purpose of reading.
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
    pub fn check_read(&self) -> Result<(), ConfigCheckError> {
        utils::dir_exists(&self.substrate_path)?;
        Ok(())
    }

    /// Checks validity of the configuration for purpose of writing.
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
    pub fn check_write(&self) -> Result<(), ConfigCheckError> {
        utils::dir_usable(&self.substrate_path)?;
        Ok(())
    }
}

/// Configuration for the `filter1` command.
#[must_use]
#[derive(Debug, Clone)]
pub struct ExtractingConfig {
    /// Path to output Wikidata cache.
    pub cache: CacheConfig,

    /// `WikidataGatherer` config.
    pub wikidata_gatherer: WikidataProducerConfig,
}

impl ExtractingConfig {
    /// Constructs a new `ExtractingConfig`.
    pub fn new(args: &commands::ExtractingArgs) -> ExtractingConfig {
        Self {
            cache: CacheConfig::new(&args.cache),
            wikidata_gatherer: WikidataProducerConfig::new_full(&args.origin),
        }
    }

    /// Checks validity of the configuration.
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        self.cache.check_write()?;
        self.wikidata_gatherer.check()?;
        Ok(())
    }
}

/// Configuration for the `filter2` command.
#[must_use]
#[derive(Debug, Clone)]
pub struct FilteringConfig {
    /// Path to output filtered .
    pub wikidata_filtered_dump_path: PathBuf,

    /// Paths to meta files.
    pub meta: MetaConfig,

    /// Paths to cache files.
    pub cache: CacheConfig,

    /// Path to the substrate.
    pub substrate_path: PathBuf,

    /// `WikidataGatherer` config.
    pub wikidata_gatherer: WikidataProducerConfig,
}

impl FilteringConfig {
    /// Constructs a new `FilteringConfig`.
    pub fn new(args: &commands::FilteringArgs) -> FilteringConfig {
        let cache = PathBuf::from(&args.cache);
        let substrate = PathBuf::from(&args.substrate);
        Self {
            wikidata_filtered_dump_path: cache.join("wikidata.jsonl"),
            meta: MetaConfig::new(&args.meta),
            cache: CacheConfig::new(&args.cache),
            substrate_path: substrate,
            wikidata_gatherer: WikidataProducerConfig::new_full(&args.origin),
        }
    }

    /// Checks validity of the configuration.
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        utils::path_creatable(&self.wikidata_filtered_dump_path)?;
        self.meta.check()?;
        self.cache.check_read()?;
        utils::dir_exists(&self.substrate_path)?;
        self.wikidata_gatherer.check()?;
        Ok(())
    }
}

#[must_use]
#[derive(Clone, Debug)]
pub struct UpdatingConfig {
    pub wikidata_gatherer: WikidataProducerConfig,
    pub off: OpenFoodFactsProducerConfig,
    pub bcorp_original_path: PathBuf,
    pub meta: MetaConfig,
    pub substrate: SubstrateConfig,
}

impl UpdatingConfig {
    /// Constructs a new `UpdatingConfig`.
    pub fn new(args: &commands::UpdatingArgs) -> UpdatingConfig {
        let origin = PathBuf::from(&args.origin);

        Self {
            wikidata_gatherer: WikidataProducerConfig::new_filtered(&args.cache),
            off: OpenFoodFactsProducerConfig::new(&args.origin),
            bcorp_original_path: origin.join("bcorp.csv"),
            meta: MetaConfig::new(&args.meta),
            substrate: SubstrateConfig::new(&args.substrate),
        }
    }

    /// Checks validity of the configuration.
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        self.wikidata_gatherer.check()?;
        self.off.check()?;
        utils::file_exists(&self.bcorp_original_path)?;
        self.meta.check()?;
        self.substrate.check_read()?;
        self.off.check()?;
        Ok(())
    }
}

/// Configuration for the `condense` command.
#[must_use]
#[derive(Debug, Clone)]
pub struct CondensationConfig {
    /// Skip substrates that require filtration.
    pub group: CondensationGroup,

    /// Paths to origin files.
    pub origin: OriginConfig,

    /// Paths to support files.
    pub support: SupportConfig,

    /// Paths to meta files.
    pub meta: MetaConfig,

    /// Paths to cache files.
    pub cache: CacheConfig,

    /// Wikidata gatherer config.
    pub wiki: WikidataProducerConfig,

    /// Open Food Facts gatherer config.
    pub off: OpenFoodFactsProducerConfig,

    /// EU Ecolabel gatherer config.
    pub eu_ecolabel: EuEcolabelProducerConfig,

    /// Substrate config.
    pub substrate: SubstrateConfig,
}

impl CondensationConfig {
    /// Constructs a new `CondensationConfig`.
    pub fn new(args: &commands::CondensationArgs) -> CondensationConfig {
        Self {
            group: args.group,
            origin: OriginConfig::new(&args.origin),
            meta: MetaConfig::new(&args.meta),
            support: SupportConfig::new(&args.support),
            cache: CacheConfig::new(&args.cache),
            wiki: WikidataProducerConfig::new_filtered(&args.cache),
            off: OpenFoodFactsProducerConfig::new(&args.origin),
            eu_ecolabel: EuEcolabelProducerConfig::new(&args.origin),
            substrate: SubstrateConfig::new(&args.substrate),
        }
    }

    /// Checks validity of the configuration.
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        self.origin.check()?;
        self.meta.check()?;
        self.support.check()?;
        self.cache.check_read()?;
        if self.group != CondensationGroup::Immediate {
            self.wiki.check()?;
        }
        self.off.check()?;
        self.eu_ecolabel.check()?;
        self.substrate.check_write()?;
        Ok(())
    }
}

/// Configuration for the `coagulate` command.
#[must_use]
#[derive(Debug, Clone)]
pub struct CoagulationConfig {
    /// Data substrate.
    pub substrate: SubstrateConfig,

    /// Runtime storage directory.
    pub runtime: PathBuf,

    /// Path to store the coagulate in.
    pub coagulate: PathBuf,
}

impl CoagulationConfig {
    /// Constructs a new `CoagulationConfig`.
    pub fn new(args: &commands::CoagulationArgs) -> CoagulationConfig {
        let coagulate = PathBuf::from(&args.coagulate);
        Self {
            substrate: SubstrateConfig::new(&args.substrate),
            runtime: coagulate.join("runtime"),
            coagulate: coagulate.join("coagulate.yaml"),
        }
    }

    /// Checks validity of the configuration.
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        self.substrate.check_read()?;
        utils::parent_creatable(&self.coagulate)?;
        Ok(())
    }
}

/// Configuration for the `crystalize` command.
#[must_use]
#[derive(Debug, Clone)]
pub struct CrystalizationConfig {
    /// Data substrate.
    pub substrate: SubstrateConfig,

    /// Path to store the coagulate in.
    pub coagulate: PathBuf,

    /// Database storage..
    pub crystal: PathBuf,

    /// Runtime storage..
    pub runtime: PathBuf,
}

impl CrystalizationConfig {
    /// Constructs a new `CondensationConfig`.
    pub fn new(args: &commands::CrystalizationArgs) -> CrystalizationConfig {
        let target = PathBuf::from(&args.target);
        let coagulate = PathBuf::from(&args.coagulate);
        Self {
            substrate: SubstrateConfig::new(&args.substrate),
            coagulate: coagulate.join("coagulate.yaml"),
            crystal: target.join("db"),
            runtime: target.join("runtime"),
        }
    }

    /// Checks validity of the configuration.
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        self.substrate.check_read()?;
        utils::file_exists(&self.coagulate)?;
        utils::parent_creatable(&self.crystal)?;
        utils::parent_creatable(&self.runtime)?;
        Ok(())
    }
}

/// Configuration for the `transcribe` command.
#[must_use]
#[derive(Clone, Debug)]
pub struct OxidationConfig {
    /// Path to the input library file.
    pub library_file_path: PathBuf,

    /// Path to the input library directory.
    pub library_dir_path: PathBuf,

    /// Path to Fashion Transparency Index data.
    pub fashion_transparency_index_path: PathBuf,

    /// Application database storage.
    pub app_storage: PathBuf,
}

impl OxidationConfig {
    //i/ Constructs a new `OxidationConfig`.
    pub fn new(args: &commands::OxidationArgs) -> OxidationConfig {
        // TODO: Read TFI data from substrate
        let support = PathBuf::from(&args.support);
        let library = PathBuf::from(&args.library);
        let target = PathBuf::from(&args.target);
        Self {
            library_file_path: library.join("library.yaml"),
            library_dir_path: library,
            fashion_transparency_index_path: support.join("fashion_transparency_index.yaml"),
            app_storage: target.join("app"),
        }
    }

    /// Checks validity of the configuration.
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        utils::file_exists(&self.library_file_path)?;
        utils::dir_exists(&self.library_dir_path)?;
        utils::file_exists(&self.fashion_transparency_index_path)?;
        utils::path_creatable(&self.app_storage)?;
        Ok(())
    }
}

/// Configuration for the `connect` command.
#[must_use]
#[derive(Clone, Debug)]
pub struct ConnectionConfig {
    /// Path to input EU Ecolabel data file.
    pub eu_ecolabel_input_path: PathBuf,

    /// Path to input Open Food Facts data file.
    pub open_food_facts_input_path: PathBuf,

    /// Path to output data file.
    pub output_path: PathBuf,

    /// `WikidataGatherer` config.
    pub wikidata_gatherer: WikidataProducerConfig,
}

impl ConnectionConfig {
    /// Constructs a new `ConnectionConfig`.
    pub fn new(args: &commands::ConnectionArgs) -> ConnectionConfig {
        let origin = PathBuf::from(&args.origin);
        let meta = PathBuf::from(&args.meta);
        Self {
            eu_ecolabel_input_path: origin.join("eu_ecolabel_products.csv"),
            open_food_facts_input_path: origin.join("en.openfoodfacts.org.products.csv"),
            output_path: meta.join("matches.yaml"),
            wikidata_gatherer: WikidataProducerConfig::new_with_path(&args.wikidata_path),
        }
    }

    /// Checks validity of the configuration.
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        utils::file_exists(&self.eu_ecolabel_input_path)?;
        utils::file_exists(&self.open_food_facts_input_path)?;
        utils::path_creatable(&self.output_path)?;
        self.wikidata_gatherer.check()?;
        Ok(())
    }
}

/// Configuration for the target part of the `sample` command.
#[must_use]
#[derive(Clone, Debug)]
pub struct SamplingTargetConfig {
    // DB storage
    pub db_storage: PathBuf,
}

/// Configuration for the backend part of the `sample` command.
#[must_use]
#[derive(Clone, Debug)]
pub struct SamplingBackendConfig {
    /// URL of the backend service.
    pub url: String,
}

/// Configuration for the `sample` command.
#[must_use]
#[derive(Clone, Debug)]
pub struct SamplingConfig {
    /// Subconfig for the target sampling.
    pub target: Option<SamplingTargetConfig>,

    /// Subconfig for the backend sampling.
    pub backend: Option<SamplingBackendConfig>,
}

impl SamplingConfig {
    /// Constructs a new `SamplingConfig`.
    pub fn new(args: &commands::SampleArgs) -> SamplingConfig {
        let target = if let Some(target) = &args.target {
            let target = PathBuf::from(target);
            Some(SamplingTargetConfig { db_storage: target.join("db") })
        } else {
            None
        };
        let backend = args.url.as_ref().map(|url| SamplingBackendConfig { url: url.clone() });
        SamplingConfig { target, backend }
    }

    /// Checks validity of the configuration.
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        if let Some(target) = &self.target {
            utils::dir_exists(&target.db_storage)?;
        }
        Ok(())
    }
}

impl From<&FullProducerConfig> for WikidataProducerConfig {
    fn from(config: &FullProducerConfig) -> WikidataProducerConfig {
        config.wiki.clone()
    }
}

impl From<&FullProducerConfig> for OpenFoodFactsProducerConfig {
    fn from(config: &FullProducerConfig) -> OpenFoodFactsProducerConfig {
        config.off.clone()
    }
}

impl From<&FullProducerConfig> for EuEcolabelProducerConfig {
    fn from(config: &FullProducerConfig) -> EuEcolabelProducerConfig {
        config.eu_ecolabel.clone()
    }
}

impl From<&CondensationConfig> for WikidataProducerConfig {
    fn from(config: &CondensationConfig) -> WikidataProducerConfig {
        config.wiki.clone()
    }
}

impl From<&CondensationConfig> for OpenFoodFactsProducerConfig {
    fn from(config: &CondensationConfig) -> OpenFoodFactsProducerConfig {
        config.off.clone()
    }
}

impl From<&UpdatingConfig> for OpenFoodFactsProducerConfig {
    fn from(config: &UpdatingConfig) -> OpenFoodFactsProducerConfig {
        config.off.clone()
    }
}

impl From<&UpdatingConfig> for WikidataProducerConfig {
    fn from(config: &UpdatingConfig) -> WikidataProducerConfig {
        config.wikidata_gatherer.clone()
    }
}

impl From<&CondensationConfig> for EuEcolabelProducerConfig {
    fn from(config: &CondensationConfig) -> EuEcolabelProducerConfig {
        config.eu_ecolabel.clone()
    }
}

impl From<&FilteringConfig> for WikidataProducerConfig {
    fn from(config: &FilteringConfig) -> WikidataProducerConfig {
        config.wikidata_gatherer.clone()
    }
}

impl From<&ExtractingConfig> for WikidataProducerConfig {
    fn from(config: &ExtractingConfig) -> WikidataProducerConfig {
        config.wikidata_gatherer.clone()
    }
}

impl From<&ConnectionConfig> for WikidataProducerConfig {
    fn from(config: &ConnectionConfig) -> WikidataProducerConfig {
        config.wikidata_gatherer.clone()
    }
}

impl From<&CrystalizationConfig> for SubstrateConfig {
    fn from(config: &CrystalizationConfig) -> SubstrateConfig {
        config.substrate.clone()
    }
}

/// Configuration for the program.
#[must_use]
#[derive(Debug, Clone)]
pub enum Config {
    Extracting(ExtractingConfig),
    Filtering(FilteringConfig),
    Updating(UpdatingConfig),
    Condensation(CondensationConfig),
    Coagulation(CoagulationConfig),
    Crystalization(CrystalizationConfig),
    Oxidation(OxidationConfig),
    Connection(ConnectionConfig),
    Sample(SamplingConfig),
}

impl Config {
    /// Constructs a new config from `Args::parse()`.
    pub fn new_from_args() -> Config {
        use commands::{Args, Commands};

        let args = Args::parse();
        match args.command {
            Commands::Extract(args) => Config::Extracting(ExtractingConfig::new(&args)),
            Commands::Filter(args) => Config::Filtering(FilteringConfig::new(&args)),
            Commands::Update(args) => Config::Updating(UpdatingConfig::new(&args)),
            Commands::Condense(args) => Config::Condensation(CondensationConfig::new(&args)),
            Commands::Coagulate(args) => Config::Coagulation(CoagulationConfig::new(&args)),
            Commands::Crystalize(args) => Config::Crystalization(CrystalizationConfig::new(&args)),
            Commands::Oxidize(args) => Config::Oxidation(OxidationConfig::new(&args)),
            Commands::Connect(args) => Config::Connection(ConnectionConfig::new(&args)),
            Commands::Sample(args) => Config::Sample(SamplingConfig::new(&args)),
        }
    }
}
