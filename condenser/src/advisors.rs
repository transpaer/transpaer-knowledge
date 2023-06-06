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
    /// Wikidata IDs of the companies, that could be found on Wikidata.
    wiki_companies: HashMap<knowledge::WikiStrId, EuEcolabelCompany>,

    /// Other companies that could not be found on Wikidata.
    other_companies: Vec<EuEcolabelCompany>,

    /// All products with GTIN.
    products: Vec<EuEcolabelProduct>,

    /// GTINs of the prodcuts.
    product_gtins: HashSet<knowledge::Gtin>,
}

impl EuEcolabelAdvisor {
    /// Constructs a new `EuEcolabelAdvisor`.
    pub fn new(
        records: &[eu_ecolabel::data::Record],
        map: &[sustainity::data::NameMatching],
    ) -> Result<Self, sustainity_wikidata::errors::ParseIdError> {
        let mut name_to_wiki = HashMap::<String, knowledge::WikiStrId>::new();
        for entry in map {
            if let Some(id) = entry.matched() {
                name_to_wiki.insert(entry.name.clone(), id);
            }
        }

        let mut name_to_vat = HashMap::<String, knowledge::VatId>::new();
        let mut wiki_companies = HashMap::<knowledge::WikiStrId, EuEcolabelCompany>::new();
        let mut other_companies = HashMap::<String, EuEcolabelCompany>::new();
        for r in records {
            // We assume each company has only one VAT number.
            if let Some(vat_number) = &r.prepare_vat_number() {
                let vat_id: knowledge::VatId = vat_number.try_into()?;
                name_to_vat.insert(r.company_name.clone(), vat_id.clone());

                let company =
                    EuEcolabelCompany { name: r.company_name.clone(), vat_id: vat_id.clone() };
                if let Some(wiki_id) = name_to_wiki.get(&r.product_or_service_name) {
                    wiki_companies.insert(wiki_id.clone(), company);
                } else {
                    other_companies.insert(r.company_name.clone(), company);
                }
            }
        }

        let other_companies: Vec<EuEcolabelCompany> = other_companies.values().cloned().collect();

        let mut products = Vec::<EuEcolabelProduct>::new();
        let mut product_gtins = HashSet::<knowledge::Gtin>::new();
        for r in records {
            if r.product_or_service == eu_ecolabel::data::ProductOrService::Product {
                let company_id: knowledge::OrganisationId = {
                    if let Some(wiki) = name_to_wiki.get(&r.company_name) {
                        wiki.clone().try_into()?
                    } else if let Some(vat_id) = name_to_vat.get(&r.company_name) {
                        vat_id.clone().into()
                    } else {
                        continue;
                    }
                };
                match r.code {
                    Some(
                        eu_ecolabel::data::Code::Ean13(ean) | eu_ecolabel::data::Code::Gtin14(ean),
                    ) => {
                        let gtin: knowledge::Gtin = ean.try_into()?;
                        product_gtins.insert(gtin.clone());
                        products.push(EuEcolabelProduct {
                            name: r.product_or_service_name.clone(),
                            gtin,
                            company_id,
                        });
                    }
                    Some(
                        eu_ecolabel::data::Code::Internal(_) | eu_ecolabel::data::Code::Other(_),
                    )
                    | None => {} // No GTIN or EAN? Then ignore.
                }
            }
        }

        Ok(Self { wiki_companies, other_companies, products, product_gtins })
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

    /// Checks if the company that can be identified with a Wikidata ID was certified.
    pub fn has_company(&self, company_id: &knowledge::WikiStrId) -> bool {
        self.wiki_companies.contains_key(company_id)
    }

    /// Returns the company that can be identified with a Wikidata ID.
    pub fn get_company(&self, company_id: &knowledge::WikiStrId) -> Option<&EuEcolabelCompany> {
        self.wiki_companies.get(company_id)
    }

    /// Returns the companies that were not found on Wikidata, and have VAT ID.
    pub fn get_other_companies(&self) -> &[EuEcolabelCompany] {
        &self.other_companies
    }

    /// Checks if at least one of the passed GTINs belongs to a certified product.
    pub fn has_product(&self, gtins: &[knowledge::Gtin]) -> bool {
        for gtin in gtins {
            if self.product_gtins.contains(gtin) {
                return true;
            }
        }
        false
    }

    /// Returns all the products that have GTIN.
    pub fn get_products(&self) -> &[EuEcolabelProduct] {
        &self.products
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
    /// Wikidata IDs of companies certifies by TCO.
    entries: HashMap<knowledge::WikiStrId, usize>,
}

impl FashionTransparencyIndexAdvisor {
    /// Constructs a new `TcoAdvisor`.
    pub fn new(
        source: &[fashion_transparency_index::data::Entry],
    ) -> Result<Self, errors::SourcesCheckError> {
        let mut repeated_ids = HashSet::<knowledge::WikiStrId>::new();
        let mut entries = HashMap::<knowledge::WikiStrId, usize>::new();
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
    pub fn has_company(&self, company_id: &knowledge::WikiStrId) -> bool {
        self.entries.contains_key(company_id)
    }

    /// Get the score for the given company.
    pub fn get_score(&self, company_id: &knowledge::WikiStrId) -> Option<usize> {
        self.entries.get(company_id).copied()
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
