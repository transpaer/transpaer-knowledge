//! Contains code ralated to parsing source data.

use std::collections::{HashMap, HashSet};

use sustainity_collecting::{
    bcorp, eu_ecolabel, fashion_transparency_index, open_food_facts, sustainity, tco,
};
use sustainity_models::write as models;

use crate::{cache, convert, errors, utils, wikidata::WikiId};

/// Holds the information read from the `BCorp` data.
pub struct BCorpAdvisor {
    /// Map from `BCorp` company domains to their names.
    domain_to_name: HashMap<String, String>,
}

impl BCorpAdvisor {
    /// Constructs a new `BCorpAdvisor`.
    #[must_use]
    pub fn new(records: &[bcorp::data::Record]) -> Self {
        let domain_to_name: HashMap<String, String> = records
            .iter()
            .map(|r| (utils::extract_domain_from_url(&r.website), r.company_name.clone()))
            .collect();
        Self { domain_to_name }
    }

    /// Loads a new `BCorpAdvisor` from a file.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub fn load(path: &std::path::Path) -> Result<Self, errors::ProcessingError> {
        if utils::is_path_ok(path) {
            let data = bcorp::reader::parse(path)?;
            Ok(Self::new(&data))
        } else {
            log::warn!("Could not access {path:?}. BCorp data won't be loaded!");
            Ok(Self::new(&[]))
        }
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

    /// Returns company certification data if a company with such domain was certified..
    #[must_use]
    pub fn get_cert_from_domains(&self, domains: &HashSet<String>) -> Option<models::BCorpCert> {
        for domain in domains {
            if let Some(name) = self.domain_to_name.get(domain) {
                return Some(models::BCorpCert { id: Self::guess_link_id_from_company_name(name) });
            }
        }
        None
    }

    /// The IDs of companies used in links to company profiles on the `BCorp` web page
    /// are not provided in the Bcorp data.
    /// Here we make a guess of how that ID looks like basing on company name.
    #[must_use]
    pub fn guess_link_id_from_company_name(name: &str) -> String {
        name.to_lowercase().replace(['.', '.'], "").replace(' ', "-")
    }
}

/// Represents a company extracted to EU Ecolabel data.
#[derive(Clone, Debug)]
pub struct EuEcolabelCompany {
    /// Company name.
    pub name: String,

    /// Company VAT ID.
    pub vat_id: models::VatId,
}

/// Represents a product extracted to EU Ecolabel data.
#[derive(Clone, Debug)]
pub struct EuEcolabelProduct {
    /// Product name.
    pub name: String,

    /// Producer ID.
    pub company_id: models::OrganisationId,

    /// GTIN of the product.
    pub gtin: models::Gtin,
}

/// Holds the information read from the `EU Ecolabel` data.
pub struct EuEcolabelAdvisor {
    /// Map from companies Vat ID to their WIkidata IDs.
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
        if utils::is_path_ok(original_path) {
            let data = eu_ecolabel::reader::parse(original_path)?;
            if utils::is_path_ok(match_path) {
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
        if utils::is_path_ok(path) {
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
                .map(|entry| ((&entry.wikidata_id).into(), entry.company_name.clone()))
                .collect(),
        }
    }

    /// Loads a new `Tcodvisor` from a file.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub fn load(path: &std::path::Path) -> Result<Self, errors::ProcessingError> {
        if utils::is_path_ok(path) {
            let data = tco::reader::parse(path)?;
            Ok(Self::new(&data))
        } else {
            log::warn!("Could not access {path:?}. TCO data won't be loaded!");
            Ok(Self::new(&[]))
        }
    }

    /// Checks if the company was certified.
    #[must_use]
    pub fn has_company(&self, company_id: &WikiId) -> bool {
        self.companies.contains_key(company_id)
    }

    /// Returns company certification data if it was certified.
    #[must_use]
    pub fn get_company_cert(&self, company_id: &WikiId) -> Option<models::TcoCert> {
        self.companies
            .get(company_id)
            .map(|brand_name| models::TcoCert { brand_name: brand_name.clone() })
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
            if let Some(id) = &entry.wikidata_id {
                let wiki_id = id.into();
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
        if utils::is_path_ok(path) {
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
    #[must_use]
    pub fn has_company(&self, company_id: &WikiId) -> bool {
        self.entries.contains_key(company_id)
    }

    /// Get the score for the given company.
    #[must_use]
    pub fn get_cert(&self, company_id: &WikiId) -> Option<models::FtiCert> {
        self.entries.get(company_id).map(|e| models::FtiCert { score: e.score })
    }

    /// Prepares Fashion Transparency Index to be presented on the Library page.
    #[must_use]
    pub fn prepare_presentation(&self) -> models::Presentation {
        let mut data = Vec::with_capacity(self.entries.len());
        for entry in self.entries.values() {
            if let Some(wikidata_id) = &entry.wikidata_id {
                data.push(models::ScoredPresentationEntry {
                    id: convert::to_org_id(wikidata_id),
                    name: entry.name.clone(),
                    score: entry.score,
                });
            }
        }
        models::Presentation {
            id: sustainity::data::LibraryTopic::CertFti.to_str().to_owned(),
            data: models::PresentationData::Scored(data),
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
}

impl WikidataAdvisor {
    /// Constructs a new `WikidataAdvisor` with loaded data.
    #[must_use]
    pub fn new(cache: &cache::Wikidata) -> Self {
        Self {
            manufacturer_ids: cache.manufacturer_ids.iter().map(From::from).collect(),
            class_ids: cache.classes.iter().map(From::from).collect(),
        }
    }

    /// Constructs a new `WikidataAdvisor` with no data.
    #[must_use]
    pub fn new_empty() -> Self {
        Self { manufacturer_ids: HashSet::new(), class_ids: HashSet::new() }
    }

    /// Loads a new `WikidataAdvisor` from a file.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub fn load<P>(path: P) -> Result<Self, errors::ProcessingError>
    where
        P: AsRef<std::path::Path> + std::fmt::Debug,
    {
        if utils::is_path_ok(path.as_ref()) {
            let data = cache::load(path.as_ref())?;
            Ok(Self::new(&data))
        } else {
            log::warn!("Could not access {path:?}. Wikidata cache won't be loaded!");
            Ok(Self::new_empty())
        }
    }

    /// Checks if the passed ID belongs to a known manufacturer.
    #[must_use]
    pub fn has_manufacturer_id(&self, id: &WikiId) -> bool {
        self.manufacturer_ids.contains(id)
    }

    /// Checks if the passed ID belongs to a known item class.
    #[must_use]
    pub fn has_class_id(&self, id: &WikiId) -> bool {
        self.class_ids.contains(id)
    }
}

/// Holds the information read from out internal data set.
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
        if utils::is_path_ok(path) {
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
    ///
    /// # Errors
    ///
    /// Returns `Err` if passed data is invalid, e.g. contains invalida IDs.
    pub fn new(map: &[sustainity::data::NameMatching]) -> Result<Self, errors::ProcessingError> {
        let mut name_to_wiki = HashMap::<String, WikiId>::new();
        for entry in map {
            if let Some(wiki_id) = entry.matched() {
                name_to_wiki.insert(entry.name.clone(), wiki_id.wiki_id.into());
            }
        }

        Ok(Self { name_to_wiki })
    }

    /// Loads a new `SustainityMatchesAdvisor` from a file.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path` or parse the contents.
    pub fn load(match_path: &std::path::Path) -> Result<Self, errors::ProcessingError> {
        if utils::is_path_ok(match_path) {
            let map = sustainity::reader::parse_id_map(match_path)?;
            Ok(Self::new(&map)?)
        } else {
            log::warn!("Could not access {match_path:?}. Sustainity match data won't be loaded!");
            Ok(Self::new(&[])?)
        }
    }

    /// Returns Wikidata ID given a name.
    #[must_use]
    pub fn name_to_wiki(&self, name: &str) -> Option<&WikiId> {
        self.name_to_wiki.get(name)
    }
}
