use std::collections::{HashMap, HashSet};

use merge::Merge;

use sustainity_collecting::{eu_ecolabel, open_food_facts};
use sustainity_wikidata::data::{Entity, Item};

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

    /// Map from prodcuts to their manufacturers.
    product_to_organisations: HashMap<knowledge::ProductId, HashSet<knowledge::OrganisationId>>,
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

    /// Links the given product to it's manufacturer.n
    pub fn link_product_to_organisations(
        &mut self,
        product: knowledge::ProductId,
        organisations: &[knowledge::OrganisationId],
    ) {
        self.product_to_organisations
            .entry(product)
            .and_modify(|o| o.extend(organisations.iter().cloned()))
            .or_insert_with(|| organisations.iter().cloned().collect());
    }
}

impl merge::Merge for CondensingCollector {
    fn merge(&mut self, other: Self) {
        utils::merge_hashmaps(&mut self.products, other.products);
        utils::merge_hashmaps(&mut self.organisations, other.organisations);
        utils::merge_hashmaps_with(
            &mut self.product_to_organisations,
            other.product_to_organisations,
            |a, b| a.extend(b.iter().cloned()),
        );
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

    /// Prepares organsation data.
    ///
    /// - fills left-over certifications
    /// - converts into a vector
    fn prepare_organisations(
        collector: &mut CondensingCollector,
        sources: &sources::FullSources,
    ) -> (Vec<knowledge::Organisation>, usize) {
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
        }

        (collector.organisations.values().cloned().collect(), num_wiki_organisations)
    }

    /// Prepares organsation keywords data.
    ///
    /// This data is needed to implement an efficient text search index.
    /// Data is composed from keyword vertex collection and edge collection connecting them to organisations.
    fn prepare_organisation_keywords(
        collector: &CondensingCollector,
    ) -> (Vec<knowledge::Keyword>, Vec<knowledge::Edge>) {
        let mut keywords = HashMap::<String, HashSet<knowledge::OrganisationId>>::new();
        for organisation in collector.organisations.values() {
            for keyword in Self::extract_keywords(&organisation.names) {
                keywords
                    .entry(keyword)
                    .and_modify(|ids| {
                        ids.insert(organisation.id.clone());
                    })
                    .or_insert_with(|| [organisation.id.clone()].into());
            }
        }

        let mut organisation_keywords = Vec::<knowledge::Keyword>::with_capacity(keywords.len());
        let mut organisation_keyword_edges = Vec::<knowledge::Edge>::new();
        for (keyword, organisation_ids) in keywords {
            let digest = md5::compute(keyword.as_bytes());
            let db_id = format!("organisation_keywords/{digest:x}");
            organisation_keywords
                .push(knowledge::Keyword { db_id: db_id.clone(), keyword: keyword.clone() });
            for organisation_id in organisation_ids {
                organisation_keyword_edges
                    .push(knowledge::Edge { from: db_id.clone(), to: organisation_id.to_db_id() });
            }
        }

        (organisation_keywords, organisation_keyword_edges)
    }

    /// Prepares product data.
    ///
    /// - fills left-over certifications
    /// - converts into a vector
    fn prepare_products(collector: &mut CondensingCollector) -> (Vec<knowledge::Product>, usize) {
        // Assign certifications to products
        let mut num_categorized_products = 0;
        for product in collector.products.values_mut() {
            if product.categories.has_category() {
                num_categorized_products += 1;
            }

            if let Some(manufacturer_ids) = collector.product_to_organisations.get(&product.id) {
                for manufacturer_id in manufacturer_ids {
                    if let Some(organisation) = collector.organisations.get(manufacturer_id) {
                        product.certifications.inherit(&organisation.certifications);
                    }
                    // TODO: There are many organisations that cannot be found.
                    //       It seems like all of them are bugs in Wikidata.
                    //       Make sure all organisations are found.
                }
            }
        }

        (collector.products.values().cloned().collect(), num_categorized_products)
    }

    /// Prepares product keywords data.
    ///
    /// This data is needed to implement and efficient text search index.
    /// Data is composed from keyword vertex collection and edge collection connecting them to products.
    fn prepare_product_keywords(
        collector: &CondensingCollector,
    ) -> (Vec<knowledge::Keyword>, Vec<knowledge::Edge>) {
        let mut keywords = HashMap::<String, HashSet<knowledge::ProductId>>::new();
        for product in collector.products.values() {
            for keyword in Self::extract_keywords(&product.names) {
                keywords
                    .entry(keyword)
                    .and_modify(|ids| {
                        ids.insert(product.id.clone());
                    })
                    .or_insert_with(|| [product.id.clone()].into());
            }
        }

        let mut product_keywords = Vec::<knowledge::Keyword>::with_capacity(keywords.len());
        let mut product_keyword_edges = Vec::<knowledge::Edge>::new();
        for (keyword, product_ids) in keywords {
            let digest = md5::compute(keyword.as_bytes());
            let db_id = format!("product_keywords/{digest:x}");
            product_keywords
                .push(knowledge::Keyword { db_id: db_id.clone(), keyword: keyword.clone() });
            for product_id in product_ids {
                product_keyword_edges
                    .push(knowledge::Edge { from: db_id.clone(), to: product_id.to_db_id() });
            }
        }

        (product_keywords, product_keyword_edges)
    }

    /// Prepares GTIN data.
    ///
    /// This data is needed to implement and efficient GTIN search index.
    /// Data is composed from GTIN vertex collection and edge collection connecting them to products.
    fn prepare_gtins(
        collector: &CondensingCollector,
    ) -> (Vec<knowledge::IdEntry>, Vec<knowledge::Edge>) {
        let mut gtins = Vec::<knowledge::IdEntry>::new();
        let mut gtin_edges = Vec::<knowledge::Edge>::new();
        for product in collector.products.values() {
            for gtin in &product.gtins {
                let db_id = gtin.to_db_id();
                gtins.push(knowledge::IdEntry { db_id: db_id.clone() });
                gtin_edges.push(knowledge::Edge { from: db_id.clone(), to: product.id.to_db_id() });
            }
        }

        (gtins, gtin_edges)
    }

    /// Prepares GTIN data.
    ///
    /// Data is domeposed from edges connecting produects to their manufacturers.
    fn prepare_manufacturing(collector: &CondensingCollector) -> Vec<knowledge::Edge> {
        let mut manufacturing_edges = Vec::<knowledge::Edge>::new();
        for (product_id, organisation_ids) in &collector.product_to_organisations {
            for organisation_id in organisation_ids {
                manufacturing_edges.push(knowledge::Edge {
                    from: organisation_id.to_db_id(),
                    to: product_id.to_db_id(),
                });
            }
        }

        manufacturing_edges
    }

    /// Prepares presentations for the Library paths.
    fn prepare_presentations(sources: &sources::FullSources) -> Vec<knowledge::Presentation> {
        vec![sources.fti.prepare_presentation()]
    }

    /// Saves organisations.
    fn save_organisations(
        organisations: (Vec<knowledge::Organisation>, usize),
        config: &config::CondensationConfig,
    ) -> Result<(), errors::ProcessingError> {
        let (mut organisations, num_wiki_organisations) = organisations;
        log::info!(
            "Saving {} organisations. ({} come from Wikidata)",
            organisations.len(),
            num_wiki_organisations
        );
        organisations.sort_by(|a, b| a.id.cmp(&b.id));
        serde_jsonlines::write_json_lines(&config.target.organisations_path, &organisations)?;
        Ok(())
    }

    // Saves organisation keywords.
    fn save_organisation_keywords(
        organisation_keywords: (Vec<knowledge::Keyword>, Vec<knowledge::Edge>),
        config: &config::CondensationConfig,
    ) -> Result<(), errors::ProcessingError> {
        let (mut organisation_keywords, mut organisation_keyword_edges) = organisation_keywords;

        log::info!("Saving {} organisation keywords", organisation_keywords.len());
        organisation_keywords.sort();
        serde_jsonlines::write_json_lines(
            &config.target.organisation_keywords_path,
            &organisation_keywords,
        )?;

        log::info!("Saving {} organisation keyword edges", organisation_keyword_edges.len());
        organisation_keyword_edges.sort();
        serde_jsonlines::write_json_lines(
            &config.target.organisation_keyword_edges_path,
            &organisation_keyword_edges,
        )?;

        Ok(())
    }

    /// Saves products.
    fn save_products(
        products: (Vec<knowledge::Product>, usize),
        config: &config::CondensationConfig,
    ) -> Result<(), errors::ProcessingError> {
        let (mut products, num_categorized_products) = products;
        log::info!(
            "Saving {} products. ({} are categorized)",
            products.len(),
            num_categorized_products
        );
        products.sort_by(|a, b| a.id.cmp(&b.id));
        serde_jsonlines::write_json_lines(&config.target.products_path, &products)?;
        Ok(())
    }

    /// Saves product keywords.
    fn save_product_keywords(
        product_keywords: (Vec<knowledge::Keyword>, Vec<knowledge::Edge>),
        config: &config::CondensationConfig,
    ) -> Result<(), errors::ProcessingError> {
        let (mut product_keywords, mut product_keyword_edges) = product_keywords;

        log::info!("Saving {} product keywords", product_keywords.len());
        product_keywords.sort();
        serde_jsonlines::write_json_lines(&config.target.product_keywords_path, &product_keywords)?;

        log::info!("Saving {} product keyword edges", product_keyword_edges.len());
        product_keyword_edges.sort();
        serde_jsonlines::write_json_lines(
            &config.target.product_keyword_edges_path,
            &product_keyword_edges,
        )?;

        Ok(())
    }

    /// Saves GTINs.
    fn save_gtins(
        gtins: (Vec<knowledge::IdEntry>, Vec<knowledge::Edge>),
        config: &config::CondensationConfig,
    ) -> Result<(), errors::ProcessingError> {
        let (mut gtins, mut gtin_edges) = gtins;

        log::info!("Saving {} product GTINs", gtins.len());
        gtins.sort();
        serde_jsonlines::write_json_lines(&config.target.gtins_path, &gtins)?;

        log::info!("Saving {} product GTIN edges", gtin_edges.len());
        gtin_edges.sort();
        serde_jsonlines::write_json_lines(&config.target.gtin_edges_path, &gtin_edges)?;

        Ok(())
    }

    /// Saves product to organisation edges.
    fn save_manufacturing(
        mut manufacturing_edges: Vec<knowledge::Edge>,
        config: &config::CondensationConfig,
    ) -> Result<(), errors::ProcessingError> {
        log::info!("Saving {} manufacturing edges", manufacturing_edges.len());
        manufacturing_edges.sort();
        serde_jsonlines::write_json_lines(
            &config.target.manufacturing_edges_path,
            &manufacturing_edges,
        )?;
        Ok(())
    }

    /// Saves presentations
    fn save_presentations(
        mut presentations: Vec<knowledge::Presentation>,
        config: &config::CondensationConfig,
    ) -> Result<(), errors::ProcessingError> {
        log::info!("Saving {} presentations", presentations.len(),);
        presentations.sort_by(|a, b| a.id.cmp(&b.id));
        serde_jsonlines::write_json_lines(&config.target.presentations_path, &presentations)?;
        Ok(())
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

        let organisations = Self::prepare_organisations(&mut collector, sources);
        let organisation_keywords = Self::prepare_organisation_keywords(&collector);
        let products = Self::prepare_products(&mut collector);
        let product_keywords = Self::prepare_product_keywords(&collector);
        let gtins = Self::prepare_gtins(&collector);
        let manufacturing_edges = Self::prepare_manufacturing(&collector);
        let presentations = Self::prepare_presentations(sources);

        Self::save_organisations(organisations, config)?;
        Self::save_organisation_keywords(organisation_keywords, config)?;
        Self::save_products(products, config)?;
        Self::save_product_keywords(product_keywords, config)?;
        Self::save_gtins(gtins, config)?;
        Self::save_manufacturing(manufacturing_edges, config)?;
        Self::save_presentations(presentations, config)?;

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
                // Gather all products
                if sources.is_product(&item) {
                    let categories = Self::extract_categories(&item);
                    if categories.has_category() || !Self::has_categories(&item, ignored::ALL) {
                        let product_id: knowledge::ProductId = item.id.to_num_id()?.into();
                        let product = knowledge::Product {
                            db_id: product_id.to_db_id(),
                            id: product_id.clone(),
                            gtins: knowledge::Gtin::convert(item.get_gtins())?,
                            names: item
                                .get_labels()
                                .into_iter()
                                .map(|label| knowledge::Text::new_wikidata(label.to_string()))
                                .collect(),
                            descriptions: item
                                .descriptions
                                .get(LANG_EN)
                                .map(|desc| vec![knowledge::Text::new_wikidata(desc.value.clone())])
                                .unwrap_or_default(),
                            images: item
                                .get_images()
                                .unwrap_or_default()
                                .iter()
                                .map(|i| knowledge::Image::new_wikidata(i.clone()))
                                .collect(),
                            categories,
                            follows: knowledge::ProductId::convert(item.get_follows())?,
                            followed_by: knowledge::ProductId::convert(item.get_followed_by())?,
                            certifications: knowledge::Certifications::default(),
                        };

                        collector.add_product(product_id.clone(), product);
                        collector.link_product_to_organisations(
                            product_id,
                            &knowledge::OrganisationId::convert(item.get_manufacturer_ids())?,
                        );
                    }
                }

                // Collect all organisations
                if sources.is_organisation(&item) {
                    let organisation_id: knowledge::OrganisationId = item.id.to_num_id()?.into();
                    let organisation = knowledge::Organisation {
                        db_id: format!("organisations/{}", organisation_id.to_string()),
                        id: item.id.clone().try_into()?,
                        vat_ids: knowledge::VatId::convert(item.get_eu_vat_numbers())?,
                        names: item
                            .get_labels()
                            .into_iter()
                            .map(|label| knowledge::Text::new_wikidata(label.to_string()))
                            .collect(),
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
                db_id: product_id.to_db_id(),
                id: product_id.clone(),
                gtins: [gtin].into(),
                names: vec![knowledge::Text::new_open_food_facts(record.product_name)],
                descriptions: Vec::default(),
                images: [knowledge::Image::new_open_food_facts(record.image_small_url)].into(),
                categories: knowledge::Categories::none(),
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
                db_id: organisation_id.to_db_id(),
                id: organisation_id.clone(),
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
                    db_id: product_id.to_db_id(),
                    id: product_id.clone(),
                    gtins: [gtin].into(),
                    names: vec![knowledge::Text::new_eu_ecolabel(record.product_or_service_name)],
                    descriptions: Vec::default(),
                    images: Vec::default(),
                    categories: knowledge::Categories::none(),
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
