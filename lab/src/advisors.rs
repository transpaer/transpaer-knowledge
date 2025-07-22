//! Contains code ralated to parsing source data.

use std::collections::{HashMap, HashSet};

use sustainity_collecting::{
    bcorp, categories::Category, eu_ecolabel, fashion_transparency_index, sustainity, tco,
};
use sustainity_models::{gather as models, ids, utils::extract_domain_from_url};
use sustainity_schema as schema;

use crate::{
    cache, convert, errors,
    substrate::Substrates,
    utils,
    wikidata::{ItemExt, WikiId},
};

/// Holds the information read from the `BCorp` data.
pub struct BCorpAdvisor {
    /// Map from `BCorp` company domains to their names.
    domain_to_name: HashMap<String, String>,

    /// Map from `BCorp` country name to country code.
    country_to_regions: HashMap<String, models::Regions>,
}

impl BCorpAdvisor {
    /// Constructs a new `BCorpAdvisor`.
    #[must_use]
    pub fn new(
        domain_to_name: HashMap<String, String>,
        country_to_regions: HashMap<String, models::Regions>,
    ) -> Self {
        Self { domain_to_name, country_to_regions }
    }

    pub fn assemble(
        records: Option<Vec<bcorp::data::Record>>,
        country_data: Option<sustainity::data::Countries>,
    ) -> Result<Self, errors::ProcessingError> {
        let domain_to_name = if let Some(records) = records {
            records
                .iter()
                .map(|r| (extract_domain_from_url(&r.website), r.company_name.clone()))
                .collect::<HashMap<String, String>>()
        } else {
            HashMap::new()
        };

        let country_to_regions = if let Some(data) = country_data {
            let mut country_to_regions = HashMap::new();
            for entry in data.countries {
                if let Some(regions) = entry.regions {
                    country_to_regions.insert(entry.tag, convert::to_model_regions(&regions)?);
                }
            }
            country_to_regions
        } else {
            HashMap::new()
        };

        Ok(Self::new(domain_to_name, country_to_regions))
    }

    /// Loads a new `BCorpAdvisor` from a file.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub fn load<P>(original_path: P, regions_path: P) -> Result<Self, errors::ProcessingError>
    where
        P: AsRef<std::path::Path>,
    {
        let path = original_path.as_ref();
        let original_data = if utils::file_exists(path).is_ok() {
            Some(sustainity_collecting::bcorp::reader::parse(path)?)
        } else {
            log::warn!(
                "Could not access `{}`. BCorp original data won't be loaded!",
                path.display(),
            );
            None
        };

        let path = regions_path.as_ref();
        let regions_data = if utils::file_exists(path).is_ok() {
            Some(sustainity::reader::parse_countries(path)?)
        } else {
            log::warn!(
                "Could not access `{}`. BCorp support data won't be loaded!",
                path.display(),
            );
            None
        };

        Self::assemble(original_data, regions_data)
    }

    /// Checks if at least one of the passed domains corresponds to a `BCorp` company.
    #[must_use]
    pub fn has_domains(&self, domains: &HashSet<String>) -> bool {
        for domain in domains {
            if self.domain_to_name.contains_key(domain) {
                return true;
            }
        }
        false
    }

    #[must_use]
    pub fn get_regions(&self, name: &str) -> Option<&models::Regions> {
        self.country_to_regions.get(name)
    }
}

/// Holds the information read from the `EU Ecolabel` data.
pub struct EuEcolabelAdvisor {
    /// Map from companies Vat ID to their Wikidata IDs.
    vat_to_wiki: HashMap<models::VatId, sustainity::data::Match>,
}

impl EuEcolabelAdvisor {
    /// Constructs a new `EuEcolabelAdvisor`.
    ///
    /// # Errors
    ///
    /// Returns `Err` the records contain invalid data, e.g. VAT number.
    pub fn new(
        records: &[eu_ecolabel::data::Record],
        map: &[sustainity::data::NameMatching],
    ) -> Result<Self, models::ParseIdError> {
        let mut name_to_wiki = HashMap::<String, sustainity::data::Match>::new();
        for entry in map {
            if let Some(wiki_match) = entry.matched() {
                name_to_wiki.insert(entry.name.clone(), wiki_match);
            }
        }

        let mut vat_to_wiki = HashMap::<models::VatId, sustainity::data::Match>::new();
        for r in records {
            // We assume each company has only one VAT number.
            if let Some(vat_number) = &r.prepare_vat_number() {
                let vat_id: models::VatId = vat_number.try_into()?;
                if let Some(wiki_match) = name_to_wiki.get(&r.product_or_service_name) {
                    vat_to_wiki.insert(vat_id, wiki_match.clone());
                }
            }
        }

        Ok(Self { vat_to_wiki })
    }

    /// Loads a new `EuEcolabelAdvisor` from a file.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path`, fails to parse the contents or the contents
    /// contain invalid data.
    pub fn load(
        original_path: &std::path::Path,
        match_path: &std::path::Path,
    ) -> Result<Self, errors::ProcessingError> {
        if utils::file_exists(original_path).is_ok() {
            let data = eu_ecolabel::reader::parse(original_path)?;
            if utils::file_exists(match_path).is_ok() {
                let map = sustainity::reader::parse_id_map(match_path)?;
                Ok(Self::new(&data, &map)?)
            } else {
                log::warn!(
                    "Could not access `{}`. Sustainity match data won't be loaded!",
                    match_path.display(),
                );
                Ok(Self::new(&[], &[])?)
            }
        } else {
            log::warn!(
                "Could not access `{}`. EU Ecolabel data won't be loaded!",
                original_path.display(),
            );
            Ok(Self::new(&[], &[])?)
        }
    }

    /// Returns Companies Wikidata ID given it VAT ID if availabel.
    #[must_use]
    pub fn vat_to_wiki(&self, vat_id: &models::VatId) -> Option<&sustainity::data::Match> {
        self.vat_to_wiki.get(vat_id)
    }
}

/// Holds the information read from the Open Food Facts data.
pub struct OpenFoodFactsAdvisor {
    /// Map from Open Food facts countries to Sustainity regionss.
    country_to_regions: HashMap<String, models::Regions>,

    /// Map from Open Food facts category tags to Sustainity categories.
    tags_to_categories: HashMap<String, HashSet<String>>,
}

impl OpenFoodFactsAdvisor {
    /// Constructs a new `OpenFoodFactsAdvisor`.
    #[must_use]
    pub fn new(
        country_to_regions: HashMap<String, models::Regions>,
        tags_to_categories: HashMap<String, HashSet<String>>,
    ) -> Self {
        Self { country_to_regions, tags_to_categories }
    }

    /// Constructs a new `OpenFoodFactsAdvisor` with loaded data.
    pub fn assemble(
        country_data: Option<sustainity::data::Countries>,
        category_data: Option<sustainity::data::Categories>,
    ) -> Result<Self, errors::ProcessingError> {
        let country_to_regions = if let Some(data) = country_data {
            let mut country_to_regions = HashMap::new();
            for entry in data.countries {
                if let Some(regions) = entry.regions {
                    country_to_regions.insert(entry.tag, convert::to_model_regions(&regions)?);
                }
            }
            country_to_regions
        } else {
            HashMap::new()
        };

        let tags_to_categories = if let Some(data) = category_data {
            let mut tags_to_categories = HashMap::new();
            for entry in data.categories {
                if entry.delete != Some(true) {
                    if let Some(categories) = entry.categories {
                        let categories = categories.iter().map(Category::get_string).collect();
                        tags_to_categories.insert(entry.tag, categories);
                    }
                }
            }
            tags_to_categories
        } else {
            HashMap::new()
        };

        Ok(Self::new(country_to_regions, tags_to_categories))
    }

    /// Loads a new `OpenFoodFactsdvisor` from a file.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub fn load<P>(country_path: P, category_path: P) -> Result<Self, errors::ProcessingError>
    where
        P: AsRef<std::path::Path>,
    {
        let path = country_path.as_ref();
        let country_data = if utils::file_exists(path).is_ok() {
            Some(sustainity::reader::parse_countries(path)?)
        } else {
            log::warn!(
                "Could not access `{}`. Open Food Facts country data won't be loaded!",
                path.display(),
            );
            None
        };

        let path = category_path.as_ref();
        let category_data = if utils::file_exists(path).is_ok() {
            Some(sustainity::reader::parse_categories(path)?)
        } else {
            log::warn!(
                "Could not access `{}`. Open Food Facts category data won't be loaded!",
                path.display(),
            );
            None
        };

        Self::assemble(country_data, category_data)
    }

    #[must_use]
    pub fn get_countries(&self, country_tag: &str) -> Option<&models::Regions> {
        self.country_to_regions.get(country_tag)
    }

    #[must_use]
    pub fn get_categories(&self, category_tag: &str) -> Option<&HashSet<String>> {
        self.tags_to_categories.get(category_tag)
    }
}

/// Holds the information read from the `TCO` data.
pub struct TcoAdvisor {
    /// Map from Wikidata IDs of companies certifies by TCO to their names.
    companies: HashMap<WikiId, String>,
}

impl TcoAdvisor {
    /// Constructs a new `TcoAdvisor`.
    #[must_use]
    pub fn new(entries: &[tco::data::Entry]) -> Self {
        Self {
            companies: entries
                .iter()
                .map(|entry| (entry.wikidata_id, entry.company_name.clone()))
                .collect(),
        }
    }

    /// Loads a new `Tcodvisor` from a file.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub fn load(path: &std::path::Path) -> Result<Self, errors::ProcessingError> {
        if utils::file_exists(path).is_ok() {
            let data = tco::reader::parse(path)?;
            Ok(Self::new(&data))
        } else {
            log::warn!("Could not access `{}`. TCO data won't be loaded!", path.display());
            Ok(Self::new(&[]))
        }
    }

    /// Checks if the company was certified.
    #[allow(clippy::trivially_copy_pass_by_ref)]
    #[must_use]
    pub fn has_company(&self, company_id: &WikiId) -> bool {
        self.companies.contains_key(company_id)
    }
}

/// Holds the information read from the `Fashion Transparency Index` data.
pub struct FashionTransparencyIndexAdvisor {
    entries: HashMap<WikiId, fashion_transparency_index::data::Entry>,
}

impl FashionTransparencyIndexAdvisor {
    /// Constructs a new `TcoAdvisor`.
    ///
    /// # Errors
    ///
    /// Returns `Err` if sources contain invalid data, e.g. repeated IDs.
    pub fn new(
        source: &[fashion_transparency_index::data::Entry],
    ) -> Result<Self, errors::SourcesCheckError> {
        let mut repeated_ids = HashSet::<WikiId>::new();
        let mut entries = HashMap::<WikiId, fashion_transparency_index::data::Entry>::new();
        for entry in source {
            if let Some(wiki_id) = entry.wikidata_id {
                if let std::collections::hash_map::Entry::Vacant(e) = entries.entry(wiki_id) {
                    e.insert(entry.clone());
                } else {
                    repeated_ids.insert(wiki_id);
                }
            }
        }

        if repeated_ids.is_empty() {
            Ok(Self { entries })
        } else {
            Err(errors::SourcesCheckError::RepeatedIds(repeated_ids))
        }
    }

    /// Loads a new `Tcodvisor` from a file.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path`, fails to parse the contents or the contents are invalid.
    pub fn load(path: &std::path::Path) -> Result<Self, errors::ProcessingError> {
        if utils::file_exists(path).is_ok() {
            let data = fashion_transparency_index::reader::parse(path)?;
            let result = Self::new(&data)?;
            Ok(result)
        } else {
            log::warn!(
                "Could not access `{}`. Fashion Transparency Index data won't be loaded!",
                path.display(),
            );
            let result = Self::new(&[])?;
            Ok(result)
        }
    }

    /// Checks if the company is known.
    #[allow(clippy::trivially_copy_pass_by_ref)]
    #[must_use]
    pub fn has_company(&self, company_id: &WikiId) -> bool {
        self.entries.contains_key(company_id)
    }

    /// Prepares Fashion Transparency Index to be presented on the Library page.
    #[must_use]
    pub fn prepare_presentation(&self) -> models::Presentation {
        let mut entries = Vec::with_capacity(self.entries.len());
        for entry in self.entries.values() {
            if let Some(wikidata_id) = entry.wikidata_id {
                entries.push(models::ScoredPresentationEntry {
                    wiki_id: wikidata_id.into(),
                    name: entry.name.clone(),
                    score: i64::from(entry.score),
                });
            }
        }
        models::Presentation {
            id: sustainity::data::LibraryTopic::CertFti.to_str().to_owned(),
            data: models::PresentationData { entries },
        }
    }
}

/// Holds the information read from the Wikidata data.
#[derive(Debug)]
pub struct WikidataAdvisor {
    /// Topic info.
    manufacturer_ids: HashSet<WikiId>,

    /// Map from Wikidata countries to Sustainity regionss.
    country_to_regions: HashMap<WikiId, models::Regions>,

    /// Map from Wikidata countries to Sustainity regionss.
    class_to_categories: HashMap<WikiId, HashSet<String>>,
}

// TODO: Introduce the `new`, `assemble`, `load` pattern for every advisor.
impl WikidataAdvisor {
    /// Constructs a new `WikidataAdvisor` with loaded data.
    pub fn new(
        manufacturer_ids: HashSet<WikiId>,
        country_to_regions: HashMap<WikiId, models::Regions>,
        class_to_categories: HashMap<WikiId, HashSet<String>>,
    ) -> Self {
        Self { manufacturer_ids, country_to_regions, class_to_categories }
    }

    /// Constructs a new `WikidataAdvisor` with loaded data.
    pub fn assemble(
        cache: Option<cache::Wikidata>,
        country_data: Option<sustainity::data::Countries>,
        category_data: Option<sustainity::data::Categories>,
    ) -> Result<Self, errors::ProcessingError> {
        let country_to_regions = if let Some(data) = country_data {
            let mut country_to_regions = HashMap::new();
            for entry in data.countries {
                if let Some(regions) = entry.regions {
                    let id = WikiId::try_from(&entry.tag)?;
                    country_to_regions.insert(id, convert::to_model_regions(&regions)?);
                }
            }
            country_to_regions
        } else {
            HashMap::new()
        };

        let class_to_categories = if let Some(data) = category_data {
            let mut class_to_categories = HashMap::new();
            for entry in data.categories {
                if entry.delete != Some(true) {
                    if let Some(categories) = entry.categories {
                        let id = WikiId::try_from(&entry.tag)?;
                        let categories = categories.iter().map(Category::get_string).collect();
                        class_to_categories.insert(id, categories);
                    }
                }
            }
            class_to_categories
        } else {
            HashMap::new()
        };

        let manufacturer_ids = if let Some(cache) = cache {
            cache.manufacturer_ids.iter().copied().collect()
        } else {
            HashSet::new()
        };

        Ok(Self::new(manufacturer_ids, country_to_regions, class_to_categories))
    }

    /// Loads a new `WikidataAdvisor` from a file.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub fn load<P>(
        cache_path: P,
        region_path: P,
        category_path: P,
    ) -> Result<Self, errors::ProcessingError>
    where
        P: AsRef<std::path::Path>,
    {
        let path = cache_path.as_ref();
        let cache = if utils::file_exists(path).is_ok() {
            Some(cache::load(path)?)
        } else {
            log::warn!("Could not access `{}`. Wikidata cache won't be loaded!", path.display());
            None
        };

        let path = region_path.as_ref();
        let region_data = if utils::file_exists(path).is_ok() {
            Some(sustainity::reader::parse_countries(path)?)
        } else {
            log::warn!("Could not access `{}`. Wikidata region won't be loaded!", path.display());
            None
        };

        let path = category_path.as_ref();
        let category_data = if utils::file_exists(path).is_ok() {
            Some(sustainity::reader::parse_categories(path)?)
        } else {
            log::warn!(
                "Could not access `{}`. Wikidata categories won't be loaded!",
                path.display()
            );
            None
        };

        Self::assemble(cache, region_data, category_data)
    }

    /// Checks if the passed ID belongs to a known manufacturer.
    #[allow(clippy::trivially_copy_pass_by_ref)]
    #[must_use]
    pub fn has_manufacturer_id(&self, id: &WikiId) -> bool {
        self.manufacturer_ids.contains(id)
    }

    #[must_use]
    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn get_regions(&self, country_id: &WikiId) -> Option<&models::Regions> {
        self.country_to_regions.get(country_id)
    }

    #[must_use]
    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn get_categories(&self, class_id: &WikiId) -> Option<&HashSet<String>> {
        self.class_to_categories.get(class_id)
    }

    #[allow(clippy::unused_self)]
    #[must_use]
    pub fn is_product(&self, item: &sustainity_wikidata::data::Item) -> bool {
        item.has_manufacturer() || item.has_gtin()
    }

    #[must_use]
    pub fn is_organisation(&self, item: &sustainity_wikidata::data::Item) -> bool {
        if self.is_product(item) {
            return false;
        }

        if item.is_organisation() {
            return true;
        }

        if self.has_manufacturer_id(&item.id) {
            return true;
        }

        false
    }
}

/// Holds the information read from the substrate data.
#[derive(Debug)]
pub struct SubstrateAdvisor {
    /// All producer wiki IDs.
    producer_wiki_ids: HashSet<ids::WikiId>,

    /// All product wiki IDs.
    product_wiki_ids: HashSet<ids::WikiId>,

    /// All domains.
    domains: HashSet<String>,
}

impl SubstrateAdvisor {
    /// Loads a new `SubstrateAdvisor` from a file with no excludes.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub fn load_all(path: &std::path::Path) -> Result<Self, errors::ProcessingError> {
        Self::load(path, &HashSet::new())
    }

    /// Loads a new `SubstrateAdvisor` from a file with specified substrates excluded.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub fn load(
        path: &std::path::Path,
        exclude: &HashSet<String>,
    ) -> Result<Self, errors::ProcessingError> {
        let mut me = Self {
            producer_wiki_ids: HashSet::new(),
            product_wiki_ids: HashSet::new(),
            domains: HashSet::new(),
        };

        if utils::dir_exists(path).is_ok() {
            log::info!("Loading SubstrateAdvisor");

            let (substrates, _report) = Substrates::prepare(path)?;
            for substrate in substrates.list() {
                if exclude.contains(&substrate.name) {
                    log::info!(" -> {} (SKIP)", substrate.name);
                    continue;
                }
                log::info!(" -> {}", substrate.name);

                match schema::read::iter_file(&substrate.path)? {
                    schema::read::FileIterVariant::Catalog(iter) => {
                        for entry in iter {
                            match entry? {
                                schema::CatalogEntry::Producer(producer) => {
                                    me.process_producer_ids(producer.ids)?;
                                }
                                schema::CatalogEntry::Product(product) => {
                                    me.process_product_ids(product.ids)?;
                                }
                            }
                        }
                    }
                    schema::read::FileIterVariant::Producer(iter) => {
                        for entry in iter {
                            match entry? {
                                schema::ProducerEntry::Product(product) => {
                                    me.process_product_ids(product.ids)?;
                                }
                                schema::ProducerEntry::Reviewer(_reviewer) => {}
                            }
                        }
                    }
                    schema::read::FileIterVariant::Review(iter) => {
                        for entry in iter {
                            match entry? {
                                schema::ReviewEntry::Producer(producer) => {
                                    me.process_producer_ids(producer.ids)?;
                                }
                                schema::ReviewEntry::Product(product) => {
                                    me.process_product_ids(product.ids)?;
                                }
                            }
                        }
                    }
                }
            }
            log::info!("Loading SubstrateAdvisor: done");
        } else {
            log::warn!("Could not access `{}`. Substrate data won't be loaded!", path.display());
        }
        Ok(me)
    }

    fn process_producer_ids(
        &mut self,
        ids: schema::ProducerIds,
    ) -> Result<(), errors::ProcessingError> {
        if let Some(wiki) = ids.wiki {
            for id in wiki {
                self.producer_wiki_ids.insert(ids::WikiId::try_from(&id)?);
            }
        }
        if let Some(domains) = ids.domains {
            self.domains.extend(domains);
        }
        Ok(())
    }

    fn process_product_ids(
        &mut self,
        ids: schema::ProductIds,
    ) -> Result<(), errors::ProcessingError> {
        if let Some(wiki) = ids.wiki {
            for id in wiki {
                self.product_wiki_ids.insert(ids::WikiId::try_from(&id)?);
            }
        }
        Ok(())
    }

    #[must_use]
    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn has_producer_wiki_id(&self, id: &ids::WikiId) -> bool {
        self.producer_wiki_ids.contains(id)
    }

    #[must_use]
    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn has_product_wiki_id(&self, id: &ids::WikiId) -> bool {
        self.product_wiki_ids.contains(id)
    }

    #[must_use]
    pub fn has_domains(&self, domains: &[String]) -> bool {
        for domain in domains {
            if self.domains.contains(domain) {
                return true;
            }
        }
        false
    }
}

/// Holds the information read from our internal data set.
pub struct SustainityLibraryAdvisor {
    /// Topic info.
    info: Vec<sustainity::data::LibraryInfo>,
}

impl SustainityLibraryAdvisor {
    /// Constructs a new `SustainityLibraryAdvisor`.
    #[must_use]
    pub fn new(info: Vec<sustainity::data::LibraryInfo>) -> Self {
        Self { info }
    }

    /// Loads a new `SustainityLibraryAdvisor` from a file.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub fn load(path: &std::path::Path) -> Result<Self, errors::ProcessingError> {
        if utils::file_exists(path).is_ok() {
            let data = sustainity::reader::parse_library(path)?;
            Ok(Self::new(data))
        } else {
            log::warn!(
                "Could not access `{}`. Sustainity library data won't be loaded!",
                path.display()
            );
            Ok(Self::new(Vec::new()))
        }
    }

    /// Returns all info.
    #[must_use]
    pub fn get_info(&self) -> &[sustainity::data::LibraryInfo] {
        &self.info
    }
}

/// Holds the informatiion about mapping from (company, brand, etc.) name to their Wikidata ID.
pub struct SustainityMatchesAdvisor {
    name_to_wiki: HashMap<String, WikiId>,
}

impl SustainityMatchesAdvisor {
    /// Constructs a new `SustainityMatchesAdvisor`.
    pub fn new(map: &[sustainity::data::NameMatching]) -> Self {
        let mut name_to_wiki = HashMap::<String, WikiId>::new();
        for entry in map {
            if let Some(wiki_id) = entry.matched() {
                name_to_wiki.insert(entry.name.clone(), wiki_id.wiki_id);
            }
        }

        Self { name_to_wiki }
    }

    /// Loads a new `SustainityMatchesAdvisor` from a file.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub fn load(match_path: &std::path::Path) -> Result<Self, errors::ProcessingError> {
        if utils::file_exists(match_path).is_ok() {
            let map = sustainity::reader::parse_id_map(match_path)?;
            Ok(Self::new(&map))
        } else {
            log::warn!(
                "Could not access `{}`. Sustainity match data won't be loaded!",
                match_path.display()
            );
            Ok(Self::new(&[]))
        }
    }

    /// Returns Wikidata ID given a name.
    #[must_use]
    pub fn name_to_wiki(&self, name: &str) -> Option<&WikiId> {
        self.name_to_wiki.get(name)
    }
}
