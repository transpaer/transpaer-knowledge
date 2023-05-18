use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use merge::Merge;

use consumers_wikidata::data::Entity;

use crate::{
    advisors, cache, categories, config, errors, knowledge,
    processing::{Collectable, Essential, Processor, Sourceable},
    utils,
    wikidata::ItemExt,
};

const LANG_EN: &str = "en";

/// Provides the core data for the processor.
#[derive(Debug)]
pub struct CondensingEssentials {
    /// Product data loader.
    products_data: consumers_wikidata::dump::Loader,

    /// Manufacturer data loader.
    manufacturers_data: consumers_wikidata::dump::Loader,
}

#[async_trait]
impl Essential for CondensingEssentials {
    type Config = config::CondensationConfig;

    fn load(config: &Self::Config) -> Result<Self, errors::ProcessingError> {
        Ok(Self {
            products_data: consumers_wikidata::dump::Loader::load(&config.source_products_path)?,
            manufacturers_data: consumers_wikidata::dump::Loader::load(
                &config.source_manufacturers_path,
            )?,
        })
    }

    async fn run(
        &mut self,
        tx: async_channel::Sender<String>,
    ) -> Result<usize, errors::ProcessingError> {
        let mut number = self.products_data.run_with_channel(tx.clone()).await?;
        number += self.manufacturers_data.run_with_channel(tx).await?;
        Ok(number)
    }
}

/// Holds all the supplementary source data.
pub struct CondensingSources {
    /// Wikidata cache.
    pub cache: cache::Wikidata,

    /// BCorp data.
    pub bcorp: advisors::BCorpAdvisor,

    /// TCO data.
    pub tco: advisors::TcoAdvisor,
}

impl Sourceable for CondensingSources {
    type Config = config::CondensationConfig;

    /// Constructs a new `CondensingSources`.
    fn load(config: &Self::Config) -> Result<Self, errors::ProcessingError> {
        let cache = cache::load(&config.wikidata_cache_path)?;

        let bcorp = advisors::BCorpAdvisor::load(&config.bcorp_path)?;
        let tco = advisors::TcoAdvisor::load(&config.tco_path)?;

        Ok(Self { cache, bcorp, tco })
    }
}

/// Data storage for gathered data.
///
/// Allows merging different instances.
#[derive(Debug, Default)]
pub struct CondensingCollector {
    /// Found products.
    products: Vec<knowledge::Product>,

    /// Found manufacturers.
    manufacturers: Vec<knowledge::Manufacturer>,
}

impl CondensingCollector {
    /// Adds a new product.
    pub fn add_product(&mut self, product: knowledge::Product) {
        self.products.push(product);
    }

    /// Adds a new manufacturer.
    pub fn add_manufacturer(&mut self, manufacturer: knowledge::Manufacturer) {
        self.manufacturers.push(manufacturer);
    }
}

impl merge::Merge for CondensingCollector {
    fn merge(&mut self, other: Self) {
        self.products.extend_from_slice(&other.products);
        self.manufacturers.extend(other.manufacturers);
    }
}

impl Collectable for CondensingCollector {}

/// Translates the filteres wikidata producern and manufacturers in to the database format.
#[derive(Debug)]
pub struct CondensingProcessor;

impl Processor for CondensingProcessor {
    type Config = config::CondensationConfig;
    type Essentials = CondensingEssentials;
    type Sources = CondensingSources;
    type Collector = CondensingCollector;

    /// Handles one Wikidata entity.
    fn handle_entity(
        _msg: &str,
        entity: &Entity,
        sources: &Self::Sources,
        collector: &mut Self::Collector,
    ) {
        match entity {
            Entity::Item(item) => {
                if let Some(name) = item.labels.get(LANG_EN).map(|label| &label.value) {
                    // Gather all manufacturer IDs and collect products
                    if item.get_manufacturer_ids().is_some() {
                        let category = if item.is_instance_of(categories::SMARTPHONE_MODEL) {
                            Some(knowledge::Category::Smartphone)
                        } else {
                            None
                        };

                        let product = knowledge::Product {
                            id: item.id.clone().into(),
                            name: name.to_string(),
                            description: item
                                .descriptions
                                .get(LANG_EN)
                                .map(|desc| desc.value.clone())
                                .unwrap_or_default(),
                            category,
                            manufacturer_ids: item.get_manufacturer_ids(),
                            follows: item.get_follows(),
                            followed_by: item.get_followed_by(),
                            certifications: knowledge::Certifications::default(),
                        };

                        collector.add_product(product);
                    }

                    // Collect all manufacturers
                    if sources.cache.has_manufacturer_id(&item.id) {
                        let websites = item.get_official_websites();
                        let domains: HashSet<String> = if let Some(websites) = &websites {
                            websites
                                .iter()
                                .map(|website| utils::extract_domain_from_url(website))
                                .collect()
                        } else {
                            HashSet::new()
                        };

                        let is_bcorp = sources.bcorp.has_domains(&domains);
                        let is_tco = sources.tco.has_company(&item.id);
                        let manufacturer = knowledge::Manufacturer {
                            id: item.id.clone().into(),
                            name: name.to_string(),
                            description: item
                                .descriptions
                                .get(LANG_EN)
                                .map(|desc| desc.value.clone())
                                .unwrap_or_default(),
                            websites: websites.unwrap_or_default(),
                            certifications: knowledge::Certifications {
                                bcorp: is_bcorp,
                                tco: is_tco,
                            },
                        };
                        collector.add_manufacturer(manufacturer);
                    }
                }
            }
            Entity::Property(_property) => (),
        }
    }

    /// Saves the result into files.
    fn save(
        config: &Self::Config,
        collector: &Self::Collector,
    ) -> Result<(), errors::ProcessingError> {
        // Assigne certifications to products.
        let manufacturer_certifications: HashMap<knowledge::Id, knowledge::Certifications> =
            collector
                .manufacturers
                .iter()
                .map(|m| (m.id.clone(), m.certifications.clone()))
                .collect();
        let mut products = collector.products.clone();
        for product in &mut products {
            if let Some(manufacturer_ids) = &product.manufacturer_ids {
                for manufacturer_id in manufacturer_ids {
                    if let Some(certifications) = manufacturer_certifications.get(manufacturer_id) {
                        product.certifications.merge(certifications.clone());
                    }
                }
            }
        }

        // Save products.
        let contents = serde_json::to_string_pretty(&products)?;
        std::fs::write(&config.target_products_path, contents)?;

        // Save manufacturers.
        let contents = serde_json::to_string_pretty(&collector.manufacturers)?;
        std::fs::write(&config.target_manufacturers_path, contents)?;

        Ok(())
    }
}
