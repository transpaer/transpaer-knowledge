//! Contains code ralated to parsing source data.

use std::collections::{HashMap, HashSet};

use sustainity_collecting::{
    bcorp, errors::IoOrSerdeError, fashion_transparency_index, sustainity, tco,
};

use crate::{cache, utils};

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
    pub fn load<P>(path: P) -> Result<Self, IoOrSerdeError>
    where
        P: AsRef<std::path::Path> + std::fmt::Debug,
    {
        if utils::is_path_ok(path.as_ref()) {
            let data = sustainity_collecting::bcorp::reader::parse(&path)?;
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
    pub fn load<P>(path: P) -> Result<Self, IoOrSerdeError>
    where
        P: AsRef<std::path::Path> + std::fmt::Debug,
    {
        if utils::is_path_ok(path.as_ref()) {
            let data = sustainity_collecting::tco::reader::parse(&path)?;
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
    pub fn new(entries: &[fashion_transparency_index::data::Entry]) -> Self {
        Self {
            entries: entries.iter().map(|entry| (entry.wikidata_id.clone(), entry.score)).collect(),
        }
    }

    /// Loads a new `Tcodvisor` from a file.
    pub fn load<P>(path: P) -> Result<Self, IoOrSerdeError>
    where
        P: AsRef<std::path::Path> + std::fmt::Debug,
    {
        if utils::is_path_ok(path.as_ref()) {
            let data = fashion_transparency_index::reader::parse(&path)?;
            Ok(Self::new(&data))
        } else {
            log::warn!(
                "Could not access {path:?}. Fashion Transparency Index data won't be loaded!"
            );
            Ok(Self::new(&[]))
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
    pub fn load<P>(path: P) -> Result<Self, IoOrSerdeError>
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
    info: Vec<sustainity::data::Info>,
}

impl SustainityAdvisor {
    /// Constructs a new `SustainityAdvisor`.
    pub fn new(info: Vec<sustainity::data::Info>) -> Self {
        Self { info }
    }

    /// Loads a new `SustainityAdvisor` from a file.
    pub fn load<P>(path: P) -> Result<Self, IoOrSerdeError>
    where
        P: AsRef<std::path::Path> + std::fmt::Debug,
    {
        if utils::is_path_ok(path.as_ref()) {
            let data = sustainity_collecting::sustainity::reader::parse(&path)?;
            Ok(Self::new(data))
        } else {
            log::warn!("Could not access {path:?}. sustainity data won't be loaded!");
            Ok(Self::new(Vec::new()))
        }
    }

    /// Returns all info.
    pub fn get_info(&self) -> &[sustainity::data::Info] {
        &self.info
    }
}
