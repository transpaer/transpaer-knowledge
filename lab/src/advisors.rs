//! Contains code ralated to parsing source data.

use std::collections::{HashMap, HashSet};

use sustainity_collecting::{
    bcorp, eu_ecolabel, fashion_transparency_index, open_food_facts, sustainity, tco,
};
use sustainity_models::{gather as models, ids, utils::extract_domain_from_url};
use sustainity_schema as schema;

use crate::{
    cache, convert, errors,
    substrate::Substrates,
    utils,
    wikidata::{self, ItemExt, WikiId},
};

/// Holds the information read from the `BCorp` data.
pub struct BCorpAdvisor {
    /// Map from `BCorp` company domains to their names.
    domain_to_name: HashMap<String, String>,

    /// Map from `BCorp` country name to country code.
    country_to_code: HashMap<String, isocountry::CountryCode>,
}

impl BCorpAdvisor {
    /// Constructs a new `BCorpAdvisor`.
    #[must_use]
    pub fn new(
        records: Option<&[bcorp::data::Record]>,
        data: Option<&crate::bcorp::data::Data>,
    ) -> Self {
        let domain_to_name = if let Some(records) = records {
            records
                .iter()
                .map(|r| (extract_domain_from_url(&r.website), r.company_name.clone()))
                .collect::<HashMap<String, String>>()
        } else {
            HashMap::new()
        };

        let country_to_code = if let Some(data) = data {
            data.countries
                .iter()
                .map(|country| (country.country.clone(), country.code))
                .collect::<HashMap<_, _>>()
        } else {
            HashMap::new()
        };

        Self { domain_to_name, country_to_code }
    }

    /// Loads a new `BCorpAdvisor` from a file.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub fn load(
        original_path: &std::path::Path,
        support_path: &std::path::Path,
    ) -> Result<Self, errors::ProcessingError> {
        let original_data = if utils::file_exists(original_path).is_ok() {
            Some(sustainity_collecting::bcorp::reader::parse(original_path)?)
        } else {
            log::warn!("Could not access {original_path:?}. BCorp original data won't be loaded!");
            None
        };

        let support_data = if utils::file_exists(support_path).is_ok() {
            Some(crate::bcorp::reader::parse(support_path)?)
        } else {
            log::warn!("Could not access {support_path:?}. BCorp support data won't be loaded!");
            None
        };

        Ok(Self::new(original_data.as_deref(), support_data.as_ref()))
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
    pub fn get_country_code(&self, name: &str) -> Option<&isocountry::CountryCode> {
        self.country_to_code.get(name)
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
                    "Could not access {match_path:?}. Sustainity match data won't be loaded!"
                );
                Ok(Self::new(&[], &[])?)
            }
        } else {
            log::warn!("Could not access {original_path:?}. EU Ecolabel data won't be loaded!");
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
}

impl OpenFoodFactsAdvisor {
    /// Constructs a new empty `OpenFoodFactsAdvisor`.
    #[must_use]
    pub fn new_empty() -> Self {
        Self { country_to_regions: HashMap::new() }
    }

    /// Constructs a new `OpenFoodFactsAdvisor`.
    #[must_use]
    pub fn new(country_to_regions: HashMap<String, models::Regions>) -> Self {
        Self { country_to_regions }
    }

    /// Loads a new `OpenFoodFactsdvisor` from a file.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub fn load(path: &std::path::Path) -> Result<Self, errors::ProcessingError> {
        if utils::file_exists(path).is_ok() {
            let data = open_food_facts::reader::parse_countries(path)?;
            let mut country_to_regions = HashMap::new();
            for entry in data {
                if let Some(regions) = entry.regions {
                    country_to_regions
                        .insert(entry.country_tag, convert::to_model_regions(&regions)?);
                }
            }
            Ok(Self::new(country_to_regions))
        } else {
            log::warn!("Could not access {path:?}. Open Food Facts data won't be loaded!");
            Ok(Self::new_empty())
        }
    }

    #[must_use]
    pub fn get_countries(&self, country_tag: &str) -> Option<&models::Regions> {
        self.country_to_regions.get(country_tag)
    }
}

/// Holds the information read from the `BCorp` data.
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
            log::warn!("Could not access {path:?}. TCO data won't be loaded!");
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
                "Could not access {path:?}. Fashion Transparency Index data won't be loaded!"
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

    /// Topic info.
    class_ids: HashSet<WikiId>,

    /// Wikidata ID to alpha3 country code
    country_to_region: HashMap<WikiId, isocountry::CountryCode>,
}

impl WikidataAdvisor {
    /// Constructs a new `WikidataAdvisor` with loaded data.
    pub fn new(
        cache: Option<&cache::Wikidata>,
        data: Option<&wikidata::support::Data>,
    ) -> Result<Self, errors::ProcessingError> {
        let country_to_region = if let Some(data) = data {
            let mut country_to_region = HashMap::new();
            for country in &data.countries {
                if let Some(code) = country.country {
                    let wiki_id = WikiId::try_from(&country.wiki_id)?;
                    country_to_region.insert(wiki_id, code);
                }
            }
            country_to_region
        } else {
            HashMap::new()
        };

        let (manufacturer_ids, class_ids) = if let Some(cache) = cache {
            (
                cache.manufacturer_ids.iter().copied().collect(),
                cache.classes.iter().copied().collect(),
            )
        } else {
            (HashSet::new(), HashSet::new())
        };

        Ok(Self { manufacturer_ids, class_ids, country_to_region })
    }

    /// Loads a new `WikidataAdvisor` from a file.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub fn load<P>(cache_path: P, source_path: P) -> Result<Self, errors::ProcessingError>
    where
        P: AsRef<std::path::Path> + std::fmt::Debug,
    {
        let cache = if utils::file_exists(cache_path.as_ref()).is_ok() {
            Some(cache::load(cache_path.as_ref())?)
        } else {
            log::warn!("Could not access {cache_path:?}. Wikidata cache won't be loaded!");
            None
        };

        let data = if utils::file_exists(source_path.as_ref()).is_ok() {
            Some(wikidata::support::parse(source_path.as_ref())?)
        } else {
            log::warn!("Could not access {source_path:?}. Wikidata source won't be loaded!");
            None
        };

        Self::new(cache.as_ref(), data.as_ref())
    }

    /// Checks if the passed ID belongs to a known manufacturer.
    #[allow(clippy::trivially_copy_pass_by_ref)]
    #[must_use]
    pub fn has_manufacturer_id(&self, id: &WikiId) -> bool {
        self.manufacturer_ids.contains(id)
    }

    /// Checks if the passed ID belongs to a known item class.
    #[allow(clippy::trivially_copy_pass_by_ref)]
    #[must_use]
    pub fn has_class_id(&self, id: &WikiId) -> bool {
        self.class_ids.contains(id)
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    #[must_use]
    pub fn get_country(&self, country_id: &WikiId) -> Option<&isocountry::CountryCode> {
        self.country_to_region.get(country_id)
    }
}

/// Holds the information read from the substrate data.
#[derive(Debug, Default)]
pub struct SubstrateAdvisor {
    /// All producer wiki IDs.
    producer_wiki_ids: HashSet<ids::WikiId>,

    /// All product wiki IDs.
    product_wiki_ids: HashSet<ids::WikiId>,

    /// All domains.
    domains: HashSet<String>,
}

impl SubstrateAdvisor {
    /// Loads a new `SubstrateAdvisor` from a file.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub fn load(path: &std::path::Path) -> Result<Self, errors::ProcessingError> {
        if utils::dir_exists(path).is_ok() {
            log::info!("Loading SubstrateAdvisor");
            let mut me = Self::default();

            let (substrates, _report) = Substrates::prepare(path)?;
            for substrate in substrates.list() {
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
            Ok(me)
        } else {
            log::warn!("Could not access {path:?}. Substrate data won't be loaded!");
            Ok(Self::default())
        }
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
            self.domains.extend(domains.into_iter());
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
    pub fn is_organisation(&self, item: &sustainity_wikidata::data::Item) -> bool {
        if self.has_producer_wiki_id(&item.id.into()) {
            return true;
        }

        if let Some(websites) = item.get_official_websites() {
            if self.has_domains(&websites) {
                return true;
            }
        }

        return false;
    }

    #[must_use]
    pub fn is_product(&self, item: &sustainity_wikidata::data::Item) -> bool {
        self.has_product_wiki_id(&item.id.into())
    }

    #[must_use]
    pub fn has_producer_wiki_id(&self, id: &ids::WikiId) -> bool {
        self.producer_wiki_ids.contains(id)
    }

    #[must_use]
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
        return false;
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
            log::warn!("Could not access {path:?}. Sustainity library data won't be loaded!");
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
            log::warn!("Could not access {match_path:?}. Sustainity match data won't be loaded!");
            Ok(Self::new(&[]))
        }
    }

    /// Returns Wikidata ID given a name.
    #[must_use]
    pub fn name_to_wiki(&self, name: &str) -> Option<&WikiId> {
        self.name_to_wiki.get(name)
    }
}
