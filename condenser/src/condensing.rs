use std::collections::{HashMap, HashSet};

use merge::Merge;

use sustainity_collecting::{eu_ecolabel, open_food_facts};
use sustainity_wikidata::data::{Entity, Item, Language};

use crate::{
    categories, config, errors, knowledge,
    processing::{Collectable, Processor},
    runners, sources, utils,
    wikidata::{ignored, ItemExt},
};

const LANG_EN: &str = "en";

/// Data storage for gathered data.
///
/// Allows merging different instances.
#[derive(Debug, Default)]
pub struct CondensingCollector {
    /// Found products.
    products: HashMap<knowledge::ProductId, knowledge::Product>,

    /// Found organisations.
    organisations: HashMap<knowledge::OrganisationId, knowledge::Organisation>,
}

impl CondensingCollector {
    /// Adds a new products.
    pub fn add_product(&mut self, id: knowledge::ProductId, product: knowledge::Product) {
        self.products.entry(id).and_modify(|e| e.merge(product.clone())).or_insert(product);
    }

    /// Adds a new organisation.
    pub fn add_organisation(
        &mut self,
        id: knowledge::OrganisationId,
        organisation: knowledge::Organisation,
    ) {
        self.organisations
            .entry(id)
            .and_modify(|e| e.merge(organisation.clone()))
            .or_insert(organisation);
    }
}

impl merge::Merge for CondensingCollector {
    fn merge(&mut self, other: Self) {
        utils::merge_hashmaps(&mut self.products, other.products);
        utils::merge_hashmaps(&mut self.organisations, other.organisations);
    }
}

impl Collectable for CondensingCollector {}

/// Translates the filteres wikidata producern and manufacturers in to the database format.
#[derive(Clone, Debug)]
pub struct CondensingProcessor;

impl CondensingProcessor {
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

    /// Extraxts keywords for DB text search from passed texts.
    fn extract_keywords(texts: &[knowledge::Text]) -> HashSet<String> {
        let mut result = HashSet::with_capacity(texts.len());
        for text in texts {
            for word in text.text.split_whitespace() {
                result.insert(word.to_lowercase());
            }
        }
        result.remove("");
        result
    }
}

impl Default for CondensingProcessor {
    fn default() -> Self {
        Self
    }
}

impl Processor for CondensingProcessor {
    type Config = config::CondensationConfig;
    type Sources = sources::FullSources;
    type Collector = CondensingCollector;

    fn initialize(
        &self,
        _collector: &mut Self::Collector,
        _sources: &Self::Sources,
        _config: &Self::Config,
    ) -> Result<(), errors::ProcessingError> {
        Ok(())
    }

    fn finalize(
        &self,
        mut collector: Self::Collector,
        sources: &Self::Sources,
        config: &Self::Config,
    ) -> Result<(), errors::ProcessingError> {
        log::info!("Finalizing...");

        // Assign certifications to organisations
        let mut num_wiki_organisations: usize = 0;
        for organisation in collector.organisations.values_mut() {
            let domains = utils::extract_domains_from_urls(&organisation.websites);

            let is_bcorp = sources.bcorp.has_domains(&domains);
            let (is_tco, fti_score) = {
                match &organisation.id {
                    knowledge::OrganisationId::Wiki(wiki_id) => {
                        let is_tco = sources.tco.has_company(&wiki_id.to_str_id());
                        let fti_score = sources.fti.get_score(&wiki_id.to_str_id());
                        num_wiki_organisations += 1;
                        (is_tco, fti_score)
                    }
                    knowledge::OrganisationId::Vat(_) => (false, None),
                }
            };

            organisation.certifications.inherit(&knowledge::Certifications {
                bcorp: is_bcorp,
                tco: is_tco,
                eu_ecolabel: false, // not updated
                fti: fti_score,
            });

            organisation.keywords = Self::extract_keywords(&organisation.names);
        }

        // Assign certifications to products
        let mut num_categorized_products = 0;
        for product in collector.products.values_mut() {
            if product.categories.has_category() {
                num_categorized_products += 1;
            }
            for manufacturer_id in &product.manufacturer_ids {
                if let Some(organisation) = collector.organisations.get(manufacturer_id) {
                    product.certifications.inherit(&organisation.certifications);
                }
            }

            product.keywords = Self::extract_keywords(&product.names);
        }

        // Prepare presentations for the Library paths.
        let mut presentations: Vec<knowledge::Presentation> =
            vec![sources.fti.prepare_presentation()];

        // Convert to vectors
        let mut products: Vec<knowledge::Product> = collector.products.into_values().collect();
        let mut organisations: Vec<knowledge::Organisation> =
            collector.organisations.into_values().collect();

        // Save products
        log::info!(
            "Saving {} products. ({} are categorized)",
            products.len(),
            num_categorized_products
        );
        products.sort_by(|a, b| a.id.cmp(&b.id));
        serde_jsonlines::write_json_lines(&config.target_products_path, &products)?;

        // Save organisations
        log::info!(
            "Saving {} organisations. ({} come from Wikidata)",
            organisations.len(),
            num_wiki_organisations
        );
        organisations.sort_by(|a, b| a.id.cmp(&b.id));
        serde_jsonlines::write_json_lines(&config.target_organisations_path, &organisations)?;

        // Save presentations
        log::info!("Saving {} presentations", presentations.len(),);
        presentations.sort_by(|a, b| a.id.cmp(&b.id));
        serde_jsonlines::write_json_lines(&config.target_presentations_path, &presentations)?;

        Ok(())
    }
}

impl runners::FullProcessor for CondensingProcessor {
    fn handle_wikidata_entity(
        &self,
        _msg: &str,
        entity: Entity,
        sources: &Self::Sources,
        collector: &mut Self::Collector,
        _config: &Self::Config,
    ) -> Result<(), errors::ProcessingError> {
        match entity {
            Entity::Item(item) => {
                if let Some(name) = item.get_label(Language::En) {
                    // Gather all products
                    if sources.is_product(&item) {
                        let categories = Self::extract_categories(&item);
                        if categories.has_category() || !Self::has_categories(&item, ignored::ALL) {
                            let product_id: knowledge::ProductId = item.id.to_num_id()?.into();
                            let product = knowledge::Product {
                                id: product_id.clone(),
                                keywords: HashSet::default(),
                                gtins: knowledge::Gtin::convert(item.get_gtins())?,
                                names: vec![knowledge::Text::new_wikidata(name.to_string())],
                                descriptions: item
                                    .descriptions
                                    .get(LANG_EN)
                                    .map(|desc| {
                                        vec![knowledge::Text::new_wikidata(desc.value.clone())]
                                    })
                                    .unwrap_or_default(),
                                images: item
                                    .get_images()
                                    .unwrap_or_default()
                                    .iter()
                                    .map(|i| knowledge::Image::new_wikidata(i.clone()))
                                    .collect(),
                                categories,
                                manufacturer_ids: knowledge::OrganisationId::convert(
                                    item.get_manufacturer_ids(),
                                )?,
                                follows: knowledge::ProductId::convert(item.get_follows())?,
                                followed_by: knowledge::ProductId::convert(item.get_followed_by())?,
                                certifications: knowledge::Certifications::default(),
                            };

                            collector.add_product(product_id, product);
                        }
                    }

                    // Collect all organisations
                    if sources.is_organisation(&item) {
                        let organisation_id: knowledge::OrganisationId =
                            item.id.to_num_id()?.into();
                        let organisation = knowledge::Organisation {
                            id: item.id.clone().try_into()?,
                            keywords: HashSet::default(),
                            vat_ids: knowledge::VatId::convert(item.get_eu_vat_numbers())?,
                            names: vec![knowledge::Text::new_wikidata(name.to_string())],
                            descriptions: item
                                .descriptions
                                .get(LANG_EN)
                                .map(|desc| vec![knowledge::Text::new_wikidata(desc.value.clone())])
                                .unwrap_or_default(),
                            images: item
                                .get_logo_images()
                                .unwrap_or_default()
                                .iter()
                                .map(|i| knowledge::Image::new_wikidata(i.clone()))
                                .collect(),
                            websites: item
                                .get_official_websites()
                                .unwrap_or_default()
                                .into_iter()
                                .collect(),
                            certifications: knowledge::Certifications::default(),
                        };
                        collector.add_organisation(organisation_id, organisation);
                    }
                }
            }
            Entity::Property(_property) => (),
        }
        Ok(())
    }

    fn handle_open_food_facts_record(
        &self,
        record: open_food_facts::data::Record,
        _sources: &Self::Sources,
        collector: &mut Self::Collector,
        _config: &Self::Config,
    ) -> Result<(), errors::ProcessingError> {
        // Some products have very long bar code.
        // Those are probably some internal bar codes, not GTINs.
        // Let's ignore them for now.
        if let Ok(gtin) = knowledge::Gtin::try_from(&record.code) {
            let product_id = knowledge::ProductId::from(gtin.clone());
            let product = knowledge::Product {
                id: product_id.clone(),
                keywords: HashSet::default(),
                gtins: [gtin].into(),
                names: vec![knowledge::Text::new_open_food_facts(record.product_name)],
                descriptions: Vec::default(),
                images: [knowledge::Image::new_open_food_facts(record.image_small_url)].into(),
                categories: knowledge::Categories::none(),
                manufacturer_ids: HashSet::default(),
                follows: HashSet::default(),
                followed_by: HashSet::default(),
                certifications: knowledge::Certifications::default(),
            };

            collector.add_product(product_id, product);
        }
        Ok(())
    }

    fn handle_eu_ecolabel_record(
        &self,
        record: eu_ecolabel::data::Record,
        sources: &Self::Sources,
        collector: &mut Self::Collector,
        _config: &Self::Config,
    ) -> Result<(), errors::ProcessingError> {
        if let Some(vat_number) = record.vat_number {
            let vat_number = knowledge::VatId::try_from(&vat_number)?;
            let organisation_id: knowledge::OrganisationId = sources
                .eu_ecolabel
                .vat_to_wiki(&vat_number)
                .map_or_else(|| vat_number.clone().into(), |id| id.clone().into());

            let organisation = knowledge::Organisation {
                id: organisation_id.clone(),
                keywords: HashSet::default(),
                vat_ids: [vat_number].into(),
                names: vec![knowledge::Text::new_eu_ecolabel(
                    record.product_or_service_name.clone(),
                )],
                descriptions: Vec::default(),
                images: Vec::default(),
                websites: HashSet::default(),
                certifications: knowledge::Certifications::new_with_eu_ecolabel(),
            };

            collector.add_organisation(organisation_id, organisation);

            if let Some(
                eu_ecolabel::data::Code::Ean13(code) | eu_ecolabel::data::Code::Gtin14(code),
            ) = record.code
            {
                let gtin = knowledge::Gtin::try_from(code)?;
                let product_id = knowledge::ProductId::from(gtin.clone());

                let product = knowledge::Product {
                    id: product_id.clone(),
                    keywords: HashSet::default(),
                    gtins: [gtin].into(),
                    names: vec![knowledge::Text::new_eu_ecolabel(record.product_or_service_name)],
                    descriptions: Vec::default(),
                    images: Vec::default(),
                    categories: knowledge::Categories::none(),
                    manufacturer_ids: HashSet::default(),
                    follows: HashSet::default(),
                    followed_by: HashSet::default(),
                    certifications: knowledge::Certifications::new_with_eu_ecolabel(),
                };

                collector.add_product(product_id, product);
            }
        }
        Ok(())
    }
}

pub type CondensingRunner = runners::FullRunner<CondensingProcessor>;
