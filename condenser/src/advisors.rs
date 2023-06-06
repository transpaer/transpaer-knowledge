//! Contains code ralated to parsing source data.

use std::collections::{HashMap, HashSet};

use sustainity_collecting::{bcorp, eu_ecolabel, fashion_transparency_index, sustainity, tco};

use crate::{cache, errors, utils};

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

/// Holds the information read from the `EU Ecolabel` data.
pub struct EuEcolabelAdvisor {
    /// Wikidata IDs of the companies with sufficient similarity score.
    known_companies: HashSet<sustainity_wikidata::data::Id>,
}

impl EuEcolabelAdvisor {
    /// Constructs a new `EuEcolabelAdvisor`.
    pub fn new(
        _records: &[eu_ecolabel::data::Record],
        map: &[sustainity::data::NameMatching],
    ) -> Self {
        let mut known_companies = HashSet::<sustainity_wikidata::data::Id>::new();
        for entry in map {
            if entry.found() {
                known_companies.insert(entry.ids[0].clone().into());
            }
        }
        Self { known_companies }
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
                Ok(Self::new(&data, &map))
            } else {
                log::warn!("Could not access {id_match_path:?}. EU Ecolabel data won't be loaded!");
                Ok(Self::new(&[], &[]))
            }
        } else {
            log::warn!("Could not access {original_path:?}. EU Ecolabel data won't be loaded!");
            Ok(Self::new(&[], &[]))
        }
    }

    /// Checks if the company was certified.
    pub fn has_company(&self, company_id: &sustainity_wikidata::data::Id) -> bool {
        self.known_companies.contains(company_id)
    }
}

/// Holds the information read from the `BCorp` data.
pub struct TcoAdvisor {
    /// Wikidata IDs of companies certifies by TCO.
    companies: HashSet<sustainity_wikidata::data::Id>,
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
    pub fn has_company(&self, company_id: &sustainity_wikidata::data::Id) -> bool {
        self.companies.contains(company_id)
    }
}

/// Holds the information read from the `Fashion Transparency Index` data.
pub struct FashionTransparencyIndexAdvisor {
    /// Wikidata IDs of companies certifies by TCO.
    entries: HashMap<sustainity_wikidata::data::Id, usize>,
}

impl FashionTransparencyIndexAdvisor {
    /// Constructs a new `TcoAdvisor`.
    pub fn new(
        source: &[fashion_transparency_index::data::Entry],
    ) -> Result<Self, errors::SourcesCheckError> {
        let mut repeated_ids = HashSet::new();
        let mut entries = HashMap::<sustainity_wikidata::data::Id, usize>::new();
        for e in source {
            if let Some(id) = &e.wikidata_id {
                if entries.contains_key(id) {
                    repeated_ids.insert(id.clone());
                } else {
                    entries.insert(id.clone(), e.score);
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
    pub fn has_company(&self, company_id: &sustainity_wikidata::data::Id) -> bool {
        self.entries.contains_key(company_id)
    }

    /// Get the score for the given company.
    pub fn get_score(&self, company_id: &sustainity_wikidata::data::Id) -> Option<usize> {
        self.entries.get(company_id).copied()
    }
}

/// Holds the information read from the Wikidata data.
#[derive(Debug)]
pub struct WikidataAdvisor {
    /// Topic info.
    manufacturer_ids: HashSet<sustainity_wikidata::data::Id>,

    /// Topic info.
    class_ids: HashSet<sustainity_wikidata::data::Id>,
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
    pub fn has_manufacturer_id(&self, id: &sustainity_wikidata::data::Id) -> bool {
        self.manufacturer_ids.contains(id)
    }

    /// Checks if the passed ID belongs to a known item class.
    pub fn has_class_id(&self, id: &sustainity_wikidata::data::Id) -> bool {
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
