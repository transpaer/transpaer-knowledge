use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use merge::Merge;

use sustainity_wikidata::data::{Entity, Item, Language};

use crate::{
    categories, config, errors, knowledge,
    processing::{Collectable, Essential, Processor},
    sources, utils,
    wikidata::{ignored, ItemExt},
};

const LANG_EN: &str = "en";

/// Provides the core data for the processor.
#[derive(Debug)]
pub struct CondensingEssentials {
    /// Product data loader.
    data: sustainity_wikidata::dump::Loader,
}

#[async_trait]
impl Essential for CondensingEssentials {
    type Config = config::CondensationConfig;

    fn load(config: &Self::Config) -> Result<Self, errors::ProcessingError> {
        Ok(Self { data: sustainity_wikidata::dump::Loader::load(&config.wikidata_source_path)? })
    }

    async fn run(
        &mut self,
        tx: async_channel::Sender<String>,
    ) -> Result<usize, errors::ProcessingError> {
        Ok(self.data.run_with_channel(tx).await?)
    }
}

/// Data storage for gathered data.
///
/// Allows merging different instances.
#[derive(Debug, Default)]
pub struct CondensingCollector {
    /// Found products.
    products: Vec<knowledge::Product>,

    /// Found organisations.
    organisations: Vec<knowledge::Organisation>,
}

impl CondensingCollector {
    /// Adds a new product.
    pub fn add_product(&mut self, product: knowledge::Product) {
        self.products.push(product);
    }

    /// Adds a new organisation.
    pub fn add_organisation(&mut self, organisation: knowledge::Organisation) {
        self.organisations.push(organisation);
    }
}

impl merge::Merge for CondensingCollector {
    fn merge(&mut self, other: Self) {
        self.products.extend_from_slice(&other.products);
        self.organisations.extend(other.organisations);
    }
}

impl Collectable for CondensingCollector {}

/// Translates the filteres wikidata producern and manufacturers in to the database format.
#[derive(Clone, Debug)]
pub struct CondensingProcessor;

impl CondensingProcessor {
    /// Constructs a new `CondensingProcessor`.
    pub fn new() -> Self {
        Self
    }

    /// Checks if the passed item is an instance of at least of one of the passed categories.
    fn has_categories(item: &Item, categories: &[&str]) -> bool {
        for category in categories {
            if item.is_instance_of(category) {
                return true;
            }
        }
        false
    }

    /// Extracts categories from an item.
    fn extract_categories(item: &Item) -> knowledge::Categories {
        knowledge::Categories {
            smartphone: Self::has_categories(item, categories::SMARTPHONE),
            smartwatch: Self::has_categories(item, categories::SMARTWATCH),
            tablet: Self::has_categories(item, categories::TABLET),
            laptop: Self::has_categories(item, categories::LAPTOP),
            computer: Self::has_categories(item, categories::COMPUTER),
            calculator: Self::has_categories(item, categories::CALCULATOR),
            game_console: Self::has_categories(item, categories::GAME_CONSOLE),
            game_controller: Self::has_categories(item, categories::GAME_CONTROLLER),
            camera: Self::has_categories(item, categories::CAMERA),
            camera_lens: Self::has_categories(item, categories::CAMERA_LENS),
            microprocessor: Self::has_categories(item, categories::MICROPROCESSOR),
            musical_instrument: Self::has_categories(item, categories::MUSICAL_INSTRUMENT),
            washing_machine: Self::has_categories(item, categories::WASHING_MACHINE),
            car: Self::has_categories(item, categories::CAR),
            motorcycle: Self::has_categories(item, categories::MOTORCYCLE),
            boat: Self::has_categories(item, categories::BOAT),
            drone: Self::has_categories(item, categories::DRONE),
            drink: Self::has_categories(item, categories::DRINK),
            food: Self::has_categories(item, categories::FOOD),
            toy: Self::has_categories(item, categories::TOY),
        }
    }
}

impl Processor for CondensingProcessor {
    type Config = config::CondensationConfig;
    type Essentials = CondensingEssentials;
    type Sources = sources::FullSources;
    type Collector = CondensingCollector;

    fn initialize(
        &self,
        _sources: &Self::Sources,
        _collector: &mut Self::Collector,
        _config: &Self::Config,
    ) -> Result<(), errors::ProcessingError> {
        Ok(())
    }

    /// Handles one Wikidata entity.
    fn handle_entity(
        &self,
        _msg: &str,
        entity: &Entity,
        sources: &Self::Sources,
        collector: &mut Self::Collector,
        _config: &Self::Config,
    ) -> Result<(), errors::ProcessingError> {
        match entity {
            Entity::Item(item) => {
                if let Some(name) = item.get_label(Language::En) {
                    // Gather all products
                    if sources.is_product(item) {
                        let categories = Self::extract_categories(item);
                        if categories.has_category() || !Self::has_categories(item, ignored::ALL) {
                            let product = knowledge::Product {
                                id: item.id.clone(),
                                name: name.to_string(),
                                description: item
                                    .descriptions
                                    .get(LANG_EN)
                                    .map(|desc| desc.value.clone())
                                    .unwrap_or_default(),
                                images: item
                                    .get_images()
                                    .unwrap_or_default()
                                    .iter()
                                    .map(|i| knowledge::Image {
                                        image: i.clone(),
                                        source: knowledge::Source::Wikidata,
                                    })
                                    .collect(),
                                categories,
                                manufacturer_ids: item.get_manufacturer_ids(),
                                follows: item.get_follows(),
                                followed_by: item.get_followed_by(),
                                certifications: knowledge::Certifications::default(),
                            };

                            collector.add_product(product);
                        }
                    }

                    // Collect all organisations
                    if sources.is_organisation(item) {
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
                        let is_eu_ecolabel = sources.eu_ecolabel.has_company(&item.id);
                        let is_tco = sources.tco.has_company(&item.id);
                        let fti_score = sources.fti.get_score(&item.id);
                        let organisation = knowledge::Organisation {
                            id: item.id.clone(),
                            name: name.to_string(),
                            description: item
                                .descriptions
                                .get(LANG_EN)
                                .map(|desc| desc.value.clone())
                                .unwrap_or_default(),
                            images: item
                                .get_logo_images()
                                .unwrap_or_default()
                                .iter()
                                .map(|i| knowledge::Image {
                                    image: i.clone(),
                                    source: knowledge::Source::Wikidata,
                                })
                                .collect(),
                            websites: websites.unwrap_or_default(),
                            certifications: knowledge::Certifications {
                                bcorp: is_bcorp,
                                eu_ecolabel: is_eu_ecolabel,
                                tco: is_tco,
                                fti: fti_score,
                            },
                        };
                        collector.add_organisation(organisation);
                    }
                }
            }
            Entity::Property(_property) => (),
        }
        Ok(())
    }

    /// Saves the result into files.
    fn finalize(
        &self,
        collector: &Self::Collector,
        config: &Self::Config,
    ) -> Result<(), errors::ProcessingError> {
        // Assigne certifications to products.
        let organisation_certifications: HashMap<knowledge::Id, knowledge::Certifications> =
            collector
                .organisations
                .iter()
                .map(|m| (m.id.clone(), m.certifications.clone()))
                .collect();
        let mut num_categorized_products = 0;
        let mut products = collector.products.clone();
        for product in &mut products {
            if product.categories.has_category() {
                num_categorized_products += 1;
            }
            if let Some(manufacturer_ids) = &product.manufacturer_ids {
                for manufacturer_id in manufacturer_ids {
                    if let Some(certifications) = organisation_certifications.get(manufacturer_id) {
                        product.certifications.merge(certifications.clone());
                    }
                }
            }
        }

        // Save products.
        log::info!(
            "Saving {} products. {} are categorized",
            products.len(),
            num_categorized_products
        );
        let contents = serde_json::to_string_pretty(&products)?;
        std::fs::write(&config.target_products_path, contents)?;

        // Save organisations.
        log::info!("Saving {} organisations", collector.organisations.len());
        let contents = serde_json::to_string_pretty(&collector.organisations)?;
        std::fs::write(&config.target_organisations_path, contents)?;

        Ok(())
    }
}
