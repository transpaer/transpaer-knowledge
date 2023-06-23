//! Contains code ralated to parsing source data.

use std::collections::{HashMap, HashSet};

use sustainity_collecting::{bcorp, eu_ecolabel, fashion_transparency_index, sustainity, tco};

use crate::{cache, errors, knowledge, utils};

/// Holds the information read from the `BCorp` data.
pub struct BCorpAdvisor {
    /// Domains of `BCorp` companies.
    domains: HashSet<String>,
}

impl BCorpAdvisor {
    /// Constructs a new `BCorpAdvisor`.
    pub fn new(records: &[bcorp::data::Record]) -> Self {
        let domains: HashSet<String> =
            records.iter().map(|r| utils::extract_domain_from_url(&r.website)).collect();
        Self { domains }
    }

    /// Loads a new `BCorpAdvisor` from a file.
    pub fn load<P>(path: P) -> Result<Self, errors::ProcessingError>
    where
        P: AsRef<std::path::Path> + std::fmt::Debug,
    {
        if utils::is_path_ok(path.as_ref()) {
            let data = bcorp::reader::parse(&path)?;
            Ok(Self::new(&data))
        } else {
            log::warn!("Could not access {path:?}. BCorp data won't be loaded!");
            Ok(Self::new(&[]))
        }
    }

    /// Checks if at least one of the passed domains corresponds to a `BCorp` company.
    pub fn has_domains(&self, domains: &HashSet<String>) -> bool {
        for domain in domains {
            if self.domains.contains(domain) {
                return true;
            }
        }
        false
    }
}

/// Represents a company extracted to EU Ecolabel data.
#[derive(Clone, Debug)]
pub struct EuEcolabelCompany {
    /// Company name.
    pub name: String,

    /// Company VAT ID.
    pub vat_id: knowledge::VatId,
}

/// Represents a product extracted to EU Ecolabel data.
#[derive(Clone, Debug)]
pub struct EuEcolabelProduct {
    /// Product name.
    pub name: String,

    /// Producer ID.
    pub company_id: knowledge::OrganisationId,

    /// GTIN of the product.
    pub gtin: knowledge::Gtin,
}

/// Holds the information read from the `EU Ecolabel` data.
pub struct EuEcolabelAdvisor {
    /// Map from companies Vat ID to their WIkidata IDs.
    vat_to_wiki: HashMap<knowledge::VatId, knowledge::WikiId>,
}

impl EuEcolabelAdvisor {
    /// Constructs a new `EuEcolabelAdvisor`.
    pub fn new(
        records: &[eu_ecolabel::data::Record],
        map: &[sustainity::data::NameMatching],
    ) -> Result<Self, sustainity_wikidata::errors::ParseIdError> {
        let mut name_to_wiki = HashMap::<String, knowledge::WikiId>::new();
        for entry in map {
            if let Some(wiki_id) = entry.matched() {
                name_to_wiki.insert(entry.name.clone(), wiki_id.to_num_id()?);
            }
        }

        let mut vat_to_wiki = HashMap::<knowledge::VatId, knowledge::WikiId>::new();
        for r in records {
            // We assume each company has only one VAT number.
            if let Some(vat_number) = &r.prepare_vat_number() {
                let vat_id: knowledge::VatId = vat_number.try_into()?;
                if let Some(wiki_id) = name_to_wiki.get(&r.product_or_service_name) {
                    vat_to_wiki.insert(vat_id, wiki_id.clone());
                }
            }
        }

        Ok(Self { vat_to_wiki })
    }

    /// Loads a new `EuEcolabelAdvisor` from a file.
    pub fn load(
        original_path: &std::path::Path,
        id_match_path: &std::path::Path,
    ) -> Result<Self, errors::ProcessingError> {
        if utils::is_path_ok(original_path) {
            let data = eu_ecolabel::reader::parse(original_path)?;
            if utils::is_path_ok(id_match_path) {
                let map = sustainity::reader::parse_id_map(id_match_path)?;
                Ok(Self::new(&data, &map)?)
            } else {
                log::warn!("Could not access {id_match_path:?}. EU Ecolabel data won't be loaded!");
                Ok(Self::new(&[], &[])?)
            }
        } else {
            log::warn!("Could not access {original_path:?}. EU Ecolabel data won't be loaded!");
            Ok(Self::new(&[], &[])?)
        }
    }

    /// Returns Companies Wikidata ID given it VAT ID if availabel.
    pub fn vat_to_wiki(&self, vat_id: &knowledge::VatId) -> Option<&knowledge::WikiId> {
        self.vat_to_wiki.get(vat_id)
    }
}

/// Holds the information read from the `BCorp` data.
pub struct TcoAdvisor {
    /// Wikidata IDs of companies certifies by TCO.
    companies: HashSet<knowledge::WikiStrId>,
}

impl TcoAdvisor {
    /// Constructs a new `TcoAdvisor`.
    pub fn new(entries: &[tco::data::Entry]) -> Self {
        Self { companies: entries.iter().map(|entry| entry.wikidata_id.clone()).collect() }
    }

    /// Loads a new `Tcodvisor` from a file.
    pub fn load<P>(path: P) -> Result<Self, errors::ProcessingError>
    where
        P: AsRef<std::path::Path> + std::fmt::Debug,
    {
        if utils::is_path_ok(path.as_ref()) {
            let data = tco::reader::parse(&path)?;
            Ok(Self::new(&data))
        } else {
            log::warn!("Could not access {path:?}. TCO data won't be loaded!");
            Ok(Self::new(&[]))
        }
    }

    /// Checks if the company was certified.
    pub fn has_company(&self, company_id: &knowledge::WikiStrId) -> bool {
        self.companies.contains(company_id)
    }
}

/// Holds the information read from the `Fashion Transparency Index` data.
pub struct FashionTransparencyIndexAdvisor {
    entries: HashMap<knowledge::WikiStrId, fashion_transparency_index::data::Entry>,
}

impl FashionTransparencyIndexAdvisor {
    /// Constructs a new `TcoAdvisor`.
    pub fn new(
        source: &[fashion_transparency_index::data::Entry],
    ) -> Result<Self, errors::SourcesCheckError> {
        let mut repeated_ids = HashSet::<knowledge::WikiId>::new();
        let mut entries =
            HashMap::<knowledge::WikiStrId, fashion_transparency_index::data::Entry>::new();
        for entry in source {
            if let Some(id) = &entry.wikidata_id {
                let str_id = id.to_str_id();
                if let std::collections::hash_map::Entry::Vacant(e) = entries.entry(str_id) {
                    e.insert(entry.clone());
                } else {
                    repeated_ids.insert(id.clone());
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
    pub fn load<P>(path: P) -> Result<Self, errors::ProcessingError>
    where
        P: AsRef<std::path::Path> + std::fmt::Debug,
    {
        if utils::is_path_ok(path.as_ref()) {
            let data = fashion_transparency_index::reader::parse(&path)?;
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
    pub fn has_company(&self, company_id: &knowledge::WikiStrId) -> bool {
        self.entries.contains_key(company_id)
    }

    /// Get the score for the given company.
    pub fn get_score(&self, company_id: &knowledge::WikiStrId) -> Option<usize> {
        self.entries.get(company_id).map(|e| e.score)
    }

    /// Prepares Fashion Transparency Index to be presented on the Library page.
    pub fn prepare_presentation(&self) -> knowledge::Presentation {
        let mut data = Vec::with_capacity(self.entries.len());
        for entry in self.entries.values() {
            if let Some(wikidata_id) = &entry.wikidata_id {
                data.push(knowledge::ScoredPresentationEntry {
                    id: wikidata_id.clone().into(),
                    name: entry.name.clone(),
                    score: entry.score,
                });
            }
        }
        knowledge::Presentation {
            id: sustainity::data::LibraryTopic::CertFti,
            data: knowledge::PresentationData::Scored(data),
        }
    }
}

/// Holds the information read from the Wikidata data.
#[derive(Debug)]
pub struct WikidataAdvisor {
    /// Topic info.
    manufacturer_ids: HashSet<knowledge::WikiStrId>,

    /// Topic info.
    class_ids: HashSet<knowledge::WikiStrId>,
}

impl WikidataAdvisor {
    /// Constructs a new `WikidataAdvisor` with loaded data.
    pub fn new(cache: &cache::Wikidata) -> Self {
        Self {
            manufacturer_ids: cache.manufacturer_ids.iter().cloned().collect(),
            class_ids: cache.classes.iter().cloned().collect(),
        }
    }

    /// Constructs a new `WikidataAdvisor` with no data.
    pub fn new_empty() -> Self {
        Self { manufacturer_ids: HashSet::new(), class_ids: HashSet::new() }
    }

    /// Loads a new `WikidataAdvisor` from a file.
    pub fn load<P>(path: P) -> Result<Self, errors::ProcessingError>
    where
        P: AsRef<std::path::Path> + std::fmt::Debug,
    {
        if utils::is_path_ok(path.as_ref()) {
            let data = cache::load(path.as_ref())?;
            Ok(Self::new(&data))
        } else {
            log::warn!(
                "Could not access {path:?}. Fashion Transparency Index data won't be loaded!"
            );
            Ok(Self::new_empty())
        }
    }

    /// Checks if the passed ID belongs to a known manufacturer.
    pub fn has_manufacturer_id(&self, id: &knowledge::WikiStrId) -> bool {
        self.manufacturer_ids.contains(id)
    }

    /// Checks if the passed ID belongs to a known item class.
    pub fn has_class_id(&self, id: &knowledge::WikiStrId) -> bool {
        self.class_ids.contains(id)
    }
}

/// Holds the information read from out internal data set.
pub struct SustainityAdvisor {
    /// Topic info.
    info: Vec<sustainity::data::LibraryInfo>,
}

impl SustainityAdvisor {
    /// Constructs a new `SustainityAdvisor`.
    pub fn new(info: Vec<sustainity::data::LibraryInfo>) -> Self {
        Self { info }
    }

    /// Loads a new `SustainityAdvisor` from a file.
    pub fn load<P>(path: P) -> Result<Self, errors::ProcessingError>
    where
        P: AsRef<std::path::Path> + std::fmt::Debug,
    {
        if utils::is_path_ok(path.as_ref()) {
            let data = sustainity::reader::parse_library(&path)?;
            Ok(Self::new(data))
        } else {
            log::warn!("Could not access {path:?}. sustainity data won't be loaded!");
            Ok(Self::new(Vec::new()))
        }
    }

    /// Returns all info.
    pub fn get_info(&self) -> &[sustainity::data::LibraryInfo] {
        &self.info
    }
}
