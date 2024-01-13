use clap::Parser;

use crate::{commands, errors::ConfigCheckError, utils};

/// Configuration for `WikidataGather`.
#[must_use]
#[derive(Debug, Clone)]
pub struct WikidataProducerConfig {
    /// Path to Wikidata data.
    pub wikidata_path: std::path::PathBuf,
}

impl WikidataProducerConfig {
    /// Constructs a new `WikidataProducerConfig` with filteresd Wikidata dump.
    pub fn new_filtered(cache: &str) -> WikidataProducerConfig {
        let cache = std::path::PathBuf::from(&cache);
        Self { wikidata_path: cache.join("wikidata.jsonl") }
    }

    /// Constructs a new `WikidataProducerConfig` with full Wikidata dump.
    pub fn new_full(origin: &str) -> WikidataProducerConfig {
        let origin = std::path::PathBuf::from(&origin);
        Self { wikidata_path: origin.join("wikidata-20231120-all.json.gz") }
    }

    /// Constructs a new `WikidataProducerConfig`.
    pub fn new_with_path(path: &str) -> WikidataProducerConfig {
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
pub struct OpenFoodFactsProducerConfig {
    /// Path to Open Food Facts data.
    pub open_food_facts_path: std::path::PathBuf,
}

impl OpenFoodFactsProducerConfig {
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
pub struct EuEcolabelProducerConfig {
    /// Path to Open Food Facts data.
    pub eu_ecolabel_path: std::path::PathBuf,
}

impl EuEcolabelProducerConfig {
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

/// Subconfiguration related to source files used by several other configs.
#[must_use]
#[derive(Debug, Clone)]
pub struct SourcesConfig {
    /// Path to input Wikidata cache.
    pub wikidata_cache_path: std::path::PathBuf,

    /// Path to B-Corp data.
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

/// Subconfiguration related to substrate files used by several other configs.
#[must_use]
#[derive(Debug, Clone)]
pub struct SubstrateConfig {
    /// Path to the substrate file directory.
    pub substrate_path: std::path::PathBuf,
}

impl SubstrateConfig {
    /// Constructs a new `SourceConfig`.
    pub fn new(substrate: &str) -> Self {
        let substrate_path = std::path::PathBuf::from(substrate);
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
        utils::empty_dir_exists(&self.substrate_path)?;
        Ok(())
    }
}

/// Configuration for the `filter1` command.
#[must_use]
#[derive(Debug, Clone)]
pub struct Filtering1Config {
    /// Path to output Wikidata cache.
    pub wikidata_cache_path: std::path::PathBuf,

    /// `WikidataGatherer` config.
    pub wikidata_gatherer: WikidataProducerConfig,
}

impl Filtering1Config {
    /// Constructs a new `Filtering1`.
    pub fn new(args: &commands::Filtering1Args) -> Filtering1Config {
        let cache = std::path::PathBuf::from(&args.cache);
        Self {
            wikidata_cache_path: cache.join("wikidata_cache.json"),
            wikidata_gatherer: WikidataProducerConfig::new_full(&args.origin),
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

/// Configuration for the `filter2` command.
#[must_use]
#[derive(Debug, Clone)]
pub struct Filtering2Config {
    /// Path to output filtered .
    pub wikidata_filtered_dump_path: std::path::PathBuf,

    /// Data sources.
    pub sources: SourcesConfig,

    /// `WikidataGatherer` config.
    pub wikidata_gatherer: WikidataProducerConfig,
}

impl Filtering2Config {
    /// Constructs a new `Filtering2Config`.
    pub fn new(args: &commands::Filtering2Args) -> Filtering2Config {
        let cache = std::path::PathBuf::from(&args.cache);
        Self {
            wikidata_filtered_dump_path: cache.join("wikidata.jsonl"),
            sources: SourcesConfig::new(&args.origin, &args.source, &args.cache),
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
        self.sources.check()?;
        self.wikidata_gatherer.check()?;
        Ok(())
    }
}

/// Configuration for the `filter` command.
#[must_use]
#[derive(Debug, Clone)]
pub struct FilteringConfig {
    /// Configuration for the first phase of filtering.
    pub filter1: Filtering1Config,

    /// Configuration for the first phase of filtering.
    pub filter2: Filtering2Config,
}

impl FilteringConfig {
    /// Constructs a new `Filtering2Config`.
    pub fn new(args: &commands::FilteringArgs) -> FilteringConfig {
        let filter1 =
            commands::Filtering1Args { origin: args.origin.clone(), cache: args.cache.clone() };
        let filter2 = commands::Filtering2Args {
            origin: args.origin.clone(),
            source: args.source.clone(),
            cache: args.cache.clone(),
        };
        Self { filter1: Filtering1Config::new(&filter1), filter2: Filtering2Config::new(&filter2) }
    }

    /// Checks validity of the configuration.
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        self.filter1.check()?;
        Ok(())
    }
}

#[must_use]
#[derive(Clone, Debug)]
pub struct UpdatingConfig {
    pub off: OpenFoodFactsProducerConfig,
    pub sources: SourcesConfig,
}

impl UpdatingConfig {
    /// Constructs a new `UpdatingConfig`.
    pub fn new(args: &commands::UpdatingArgs) -> UpdatingConfig {
        Self {
            off: OpenFoodFactsProducerConfig::new(&args.origin),
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

/// Configuration for the `condense` command.
#[must_use]
#[derive(Debug, Clone)]
pub struct CondensationConfig {
    /// Data sources.
    pub sources: SourcesConfig,

    /// Full producer config.
    pub full_producer: FullProducerConfig,

    /// Substrate config.
    pub substrate: SubstrateConfig,
}

impl CondensationConfig {
    /// Constructs a new `CondensationConfig`.
    pub fn new(args: &commands::CondensationArgs) -> CondensationConfig {
        Self {
            sources: SourcesConfig::new(&args.origin, &args.source, &args.cache),
            full_producer: FullProducerConfig::new(&args.origin, &args.cache),
            substrate: SubstrateConfig::new(&args.substrate),
        }
    }

    /// Checks validity of the configuration.
    ///
    /// # Errors
    ///
    /// Returns `Err` if paths expected to exist do not exist or paths expected to not exist do exist.
    pub fn check(&self) -> Result<(), ConfigCheckError> {
        self.sources.check()?;
        self.full_producer.check()?;
        self.substrate.check_write()?;
        Ok(())
    }
}

/// Part of the configuration for the `crystalize` command.
///
/// Extracted to keep size of all configuration structures similar.
#[derive(Debug, Clone)]
pub struct TargetConfig {
    pub organisations_path: std::path::PathBuf,
    pub organisation_keywords_path: std::path::PathBuf,
    pub organisation_keyword_edges_path: std::path::PathBuf,
    pub organisation_vat_ids_path: std::path::PathBuf,
    pub organisation_vat_id_edges_path: std::path::PathBuf,
    pub organisation_wiki_ids_path: std::path::PathBuf,
    pub organisation_wiki_id_edges_path: std::path::PathBuf,
    pub organisation_wwws_path: std::path::PathBuf,
    pub organisation_www_edges_path: std::path::PathBuf,
    pub products_path: std::path::PathBuf,
    pub product_keywords_path: std::path::PathBuf,
    pub product_keyword_edges_path: std::path::PathBuf,
    pub product_eans_path: std::path::PathBuf,
    pub product_ean_edges_path: std::path::PathBuf,
    pub product_gtins_path: std::path::PathBuf,
    pub product_gtin_edges_path: std::path::PathBuf,
    pub product_wiki_ids_path: std::path::PathBuf,
    pub product_wiki_id_edges_path: std::path::PathBuf,
    pub categories_path: std::path::PathBuf,
    pub category_edges_path: std::path::PathBuf,
    pub manufacturing_edges_path: std::path::PathBuf,
}

/// Configuration for the `crystalize` command.
#[must_use]
#[derive(Debug, Clone)]
pub struct CrystalizationConfig {
    /// Output file paths.
    pub target: Box<TargetConfig>,

    /// Local storage for runtime operations.
    pub local_storage_runtime: std::path::PathBuf,

    /// Data substrate.
    pub substrate: SubstrateConfig,
}

impl CrystalizationConfig {
    /// Constructs a new `CondensationConfig`.
    pub fn new(args: &commands::CrystalizationArgs) -> CrystalizationConfig {
        let target = std::path::PathBuf::from(&args.target);
        Self {
            target: Box::new(TargetConfig {
                organisations_path: target.join("organisations.jsonl"),
                organisation_keywords_path: target.join("organisation_keywords.jsonl"),
                organisation_keyword_edges_path: target.join("organisation_keyword_edges.jsonl"),
                organisation_vat_ids_path: target.join("organisation_vat_ids.jsonl"),
                organisation_vat_id_edges_path: target.join("organisation_vat_id_edges.jsonl"),
                organisation_wiki_ids_path: target.join("organisation_wiki_ids.jsonl"),
                organisation_wiki_id_edges_path: target.join("organisation_wiki_id_edges.jsonl"),
                organisation_wwws_path: target.join("organisation_wwws.jsonl"),
                organisation_www_edges_path: target.join("organisation_www_edges.jsonl"),
                products_path: target.join("products.jsonl"),
                product_keywords_path: target.join("product_keywords.jsonl"),
                product_keyword_edges_path: target.join("product_keyword_edges.jsonl"),
                product_eans_path: target.join("product_eans.jsonl"),
                product_ean_edges_path: target.join("product_ean_edges.jsonl"),
                product_gtins_path: target.join("product_gtins.jsonl"),
                product_gtin_edges_path: target.join("product_gtin_edges.jsonl"),
                product_wiki_ids_path: target.join("product_wiki_ids.jsonl"),
                product_wiki_id_edges_path: target.join("product_wiki_id_edges.jsonl"),
                categories_path: target.join("categories.jsonl"),
                category_edges_path: target.join("category_edges.jsonl"),
                manufacturing_edges_path: target.join("manufacturing_edges.jsonl"),
            }),
            local_storage_runtime: target.join("local_storage_runtime"),
            substrate: SubstrateConfig::new(&args.substrate),
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
        utils::path_creatable(&self.target.organisation_vat_ids_path)?;
        utils::path_creatable(&self.target.organisation_vat_id_edges_path)?;
        utils::path_creatable(&self.target.organisation_wiki_ids_path)?;
        utils::path_creatable(&self.target.organisation_wiki_id_edges_path)?;
        utils::path_creatable(&self.target.organisation_wwws_path)?;
        utils::path_creatable(&self.target.organisation_www_edges_path)?;
        utils::path_creatable(&self.target.products_path)?;
        utils::path_creatable(&self.target.product_keywords_path)?;
        utils::path_creatable(&self.target.product_keyword_edges_path)?;
        utils::path_creatable(&self.target.product_eans_path)?;
        utils::path_creatable(&self.target.product_ean_edges_path)?;
        utils::path_creatable(&self.target.product_gtins_path)?;
        utils::path_creatable(&self.target.product_gtin_edges_path)?;
        utils::path_creatable(&self.target.product_wiki_ids_path)?;
        utils::path_creatable(&self.target.product_wiki_id_edges_path)?;
        utils::path_creatable(&self.target.categories_path)?;
        utils::path_creatable(&self.target.category_edges_path)?;
        utils::path_creatable(&self.target.manufacturing_edges_path)?;
        self.substrate.check_read()?;
        utils::path_creatable(&self.local_storage_runtime)?;
        Ok(())
    }
}

/// Configuration for the `transcribe` command.
#[must_use]
#[derive(Clone, Debug)]
pub struct OxidationConfig {
    /// Path to the input library file.
    pub library_file_path: std::path::PathBuf,

    /// Path to the input library directory.
    pub library_dir_path: std::path::PathBuf,

    /// Path to the output library file.
    pub library_target_path: std::path::PathBuf,

    /// Path to Fashion Transparency Index data.
    pub fashion_transparency_index_path: std::path::PathBuf,

    /// Path to the output presentations file.
    pub presentations_path: std::path::PathBuf,
}

impl OxidationConfig {
    //i/ Constructs a new `OxidationConfig`.
    pub fn new(args: &commands::OxidationArgs) -> OxidationConfig {
        let source = std::path::PathBuf::from(&args.source);
        let library = std::path::PathBuf::from(&args.library);
        let target = std::path::PathBuf::from(&args.target);
        Self {
            library_file_path: source.join("sustainity_library.yaml"),
            library_dir_path: library,
            library_target_path: target.join("library.jsonl"),
            fashion_transparency_index_path: source.join("fashion_transparency_index.yaml"),
            presentations_path: target.join("presentations.jsonl"),
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
        utils::path_exists(&self.fashion_transparency_index_path)?;
        utils::path_creatable(&self.presentations_path)?;
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
    pub wikidata_gatherer: WikidataProducerConfig,
}

impl AnalysisConfig {
    /// Constructs a new `FilteringConfig`.
    pub fn new(args: &commands::AnalysisArgs) -> AnalysisConfig {
        let cache = std::path::PathBuf::from(&args.cache);
        Self {
            wikidata_cache_path: cache.join("wikidata_cache.json"),
            wikidata_gatherer: WikidataProducerConfig::new_filtered(&args.cache),
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
    pub wikidata_gatherer: WikidataProducerConfig,
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
            wikidata_gatherer: WikidataProducerConfig::new_with_path(&args.wikidata_path),
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

/// Configuration for the target part of the `sample` command.
#[must_use]
#[derive(Clone, Debug)]
pub struct SamplingTargetConfig {
    /// Path to the product Wiki IDs data file.
    pub product_wiki_ids_path: std::path::PathBuf,

    /// Path to the product Wiki ID edges data file.
    pub product_wiki_id_edges_path: std::path::PathBuf,

    /// Path to the products data file.
    pub products_path: std::path::PathBuf,

    /// Path to the organisation Wiki IDs data file.
    pub organisation_wiki_ids_path: std::path::PathBuf,

    /// Path to the organisation Wiki ID edges data file.
    pub organisation_wiki_id_edges_path: std::path::PathBuf,

    /// Path to the organisations data file.
    pub organisations_path: std::path::PathBuf,
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
            let target = std::path::PathBuf::from(target);
            Some(SamplingTargetConfig {
                product_wiki_ids_path: target.join("product_wiki_ids.jsonl"),
                product_wiki_id_edges_path: target.join("product_wiki_id_edges.jsonl"),
                products_path: target.join("products.jsonl"),
                organisation_wiki_ids_path: target.join("organisation_wiki_ids.jsonl"),
                organisation_wiki_id_edges_path: target.join("organisation_wiki_id_edges.jsonl"),
                organisations_path: target.join("organisations.jsonl"),
            })
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
            utils::path_exists(&target.products_path)?;
            utils::path_exists(&target.organisations_path)?;
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

impl From<&CondensationConfig> for FullProducerConfig {
    fn from(config: &CondensationConfig) -> FullProducerConfig {
        config.full_producer.clone()
    }
}

impl From<&CondensationConfig> for WikidataProducerConfig {
    fn from(config: &CondensationConfig) -> WikidataProducerConfig {
        config.full_producer.wiki.clone()
    }
}

impl From<&CondensationConfig> for OpenFoodFactsProducerConfig {
    fn from(config: &CondensationConfig) -> OpenFoodFactsProducerConfig {
        config.full_producer.off.clone()
    }
}

impl From<&UpdatingConfig> for OpenFoodFactsProducerConfig {
    fn from(config: &UpdatingConfig) -> OpenFoodFactsProducerConfig {
        config.off.clone()
    }
}

impl From<&CondensationConfig> for EuEcolabelProducerConfig {
    fn from(config: &CondensationConfig) -> EuEcolabelProducerConfig {
        config.full_producer.eu_ecolabel.clone()
    }
}

impl From<&Filtering2Config> for WikidataProducerConfig {
    fn from(config: &Filtering2Config) -> WikidataProducerConfig {
        config.wikidata_gatherer.clone()
    }
}

impl From<&Filtering1Config> for WikidataProducerConfig {
    fn from(config: &Filtering1Config) -> WikidataProducerConfig {
        config.wikidata_gatherer.clone()
    }
}

impl From<&AnalysisConfig> for WikidataProducerConfig {
    fn from(config: &AnalysisConfig) -> WikidataProducerConfig {
        config.wikidata_gatherer.clone()
    }
}

impl From<&ConnectionConfig> for WikidataProducerConfig {
    fn from(config: &ConnectionConfig) -> WikidataProducerConfig {
        config.wikidata_gatherer.clone()
    }
}

impl From<&CondensationConfig> for SourcesConfig {
    fn from(config: &CondensationConfig) -> SourcesConfig {
        config.sources.clone()
    }
}

impl From<&Filtering2Config> for SourcesConfig {
    fn from(config: &Filtering2Config) -> SourcesConfig {
        config.sources.clone()
    }
}

impl From<&UpdatingConfig> for SourcesConfig {
    fn from(config: &UpdatingConfig) -> SourcesConfig {
        config.sources.clone()
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
    Filtering1(Filtering1Config),
    Filtering2(Filtering2Config),
    Filtering(FilteringConfig),
    Updating(UpdatingConfig),
    Condensation(CondensationConfig),
    Crystalization(CrystalizationConfig),
    Oxidation(OxidationConfig),
    Analysis(AnalysisConfig),
    Connection(ConnectionConfig),
    Sample(SamplingConfig),
}

impl Config {
    /// Constructs a new config from `Args::parse()`.
    pub fn new_from_args() -> Config {
        use commands::{Args, Commands};

        let args = Args::parse();
        match args.command {
            Commands::Filter1(args) => Config::Filtering1(Filtering1Config::new(&args)),
            Commands::Filter2(args) => Config::Filtering2(Filtering2Config::new(&args)),
            Commands::Filter(args) => Config::Filtering(FilteringConfig::new(&args)),
            Commands::Update(args) => Config::Updating(UpdatingConfig::new(&args)),
            Commands::Condense(args) => Config::Condensation(CondensationConfig::new(&args)),
            Commands::Crystalize(args) => Config::Crystalization(CrystalizationConfig::new(&args)),
            Commands::Oxidize(args) => Config::Oxidation(OxidationConfig::new(&args)),
            Commands::Analyze(args) => Config::Analysis(AnalysisConfig::new(&args)),
            Commands::Connect(args) => Config::Connection(ConnectionConfig::new(&args)),
            Commands::Sample(args) => Config::Sample(SamplingConfig::new(&args)),
        }
    }
}
