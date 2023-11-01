use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use merge::Merge;

use sustainity_collecting::{eu_ecolabel, open_food_facts};
use sustainity_models::write as models;
use sustainity_wikidata::data::{Entity, Item};

use crate::{
    advisors, categories, config, convert, errors, parallel, runners, sources,
    sources::Sourceable,
    utils,
    wikidata::{ignored, ItemExt, WikiId},
};

const LANG_EN: &str = "en";
const MAX_CATEGORY_PRODUCT_NUM: usize = 300_000;

/// Data storage for gathered data.
///
/// Allows merging different instances.
#[derive(Debug, Default, Clone)]
pub struct CondensingCollector {
    /// Found products.
    products: HashMap<models::ProductId, models::Product>,

    /// Found organisations.
    organisations: HashMap<models::OrganisationId, models::Organisation>,

    /// Map from prodcuts to their manufacturers.
    product_to_organisations: HashMap<models::ProductId, HashSet<models::OrganisationId>>,

    /// Map from products to regions where they are available.
    product_to_regions: HashMap<models::ProductId, models::Regions>,

    /// Map from products to categories.
    product_to_categories: HashMap<models::ProductId, HashSet<String>>,

    /// Map from categories to products.
    category_to_products: HashMap<String, HashSet<models::ProductId>>,
}

impl CondensingCollector {
    /// Adds a new products.
    pub fn add_product(&mut self, id: models::ProductId, product: models::Product) {
        self.products.entry(id).and_modify(|e| e.merge(product.clone())).or_insert(product);
    }

    /// Adds a new organisation.
    pub fn add_organisation(
        &mut self,
        id: models::OrganisationId,
        organisation: models::Organisation,
    ) {
        self.organisations
            .entry(id)
            .and_modify(|e| e.merge(organisation.clone()))
            .or_insert(organisation);
    }

    /// Links the given product to it's manufacturer.
    pub fn link_product_to_organisations(
        &mut self,
        product_id: models::ProductId,
        organisations: &[models::OrganisationId],
    ) {
        self.product_to_organisations
            .entry(product_id)
            .and_modify(|o| o.extend(organisations.iter().cloned()))
            .or_insert_with(|| organisations.iter().cloned().collect());
    }

    /// Links the given product to categories.
    pub fn link_product_to_categories(
        &mut self,
        product_id: &models::ProductId,
        categories: &[String],
    ) {
        for category in categories {
            self.category_to_products
                .entry(category.to_string())
                .and_modify(|products| {
                    products.insert(product_id.clone());
                })
                .or_insert_with(|| [product_id.clone()].into());
        }
        self.product_to_categories
            .entry(product_id.clone())
            .and_modify(|values| {
                values.extend(categories.iter().cloned());
            })
            .or_insert_with(|| categories.iter().cloned().collect());
    }

    /// Links the given product to regions where they are available.
    pub fn link_product_to_sell_regions(
        &mut self,
        product_id: &models::ProductId,
        regions: models::Regions,
    ) {
        self.product_to_regions.insert(product_id.clone(), regions);
    }
}

impl merge::Merge for CondensingCollector {
    fn merge(&mut self, other: Self) {
        utils::merge_hashmaps(&mut self.products, other.products);
        utils::merge_hashmaps(&mut self.organisations, other.organisations);
        utils::merge_hashmaps_with(
            &mut self.product_to_organisations,
            other.product_to_organisations,
            std::iter::Extend::extend,
        );
        utils::merge_hashmaps_with(
            &mut self.product_to_regions,
            other.product_to_regions,
            merge::Merge::merge,
        );
        utils::merge_hashmaps_with(
            &mut self.product_to_categories,
            other.product_to_categories,
            std::iter::Extend::extend,
        );
        utils::merge_hashmaps_with(
            &mut self.category_to_products,
            other.category_to_products,
            std::iter::Extend::extend,
        );
    }
}

#[derive(Clone)]
pub struct CondensingWorker {
    collector: CondensingCollector,
    sources: Arc<sources::FullSources>,
}

impl CondensingWorker {
    #[must_use]
    pub fn new(sources: Arc<sources::FullSources>) -> Self {
        Self { collector: CondensingCollector::default(), sources }
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

    /// Extracts categories from a Wikidata item.
    fn extract_wikidata_categories(item: &Item) -> Vec<String> {
        let mut result = Vec::new();
        for (name, wiki_categories) in categories::CATEGORIES {
            if Self::has_categories(item, wiki_categories) {
                result.push((*name).to_string());
            }
        }
        result
    }

    /// Extracts categories from a Wikidata item.
    fn extract_open_food_facts_categories(record: &open_food_facts::data::Record) -> Vec<String> {
        let mut result = Vec::<String>::new();
        for tag in record.food_groups_tags.split(',') {
            if tag.len() > 3 && tag.starts_with("en:") {
                result.push(tag[3..].to_string());
            }
        }
        result
    }

    /// Extracts sell regions from Open Food Facts record.
    fn extract_open_food_facts_sell_regions(
        record: &open_food_facts::data::Record,
        off: &advisors::OpenFoodFactsAdvisor,
    ) -> models::Regions {
        let mut result = HashSet::<isocountry::CountryCode>::new();
        for tag in record.extract_sell_countries() {
            match off.get_countries(&tag) {
                Some(models::Regions::World) => return models::Regions::World,
                Some(models::Regions::List(list)) => result.extend(list.iter()),
                Some(models::Regions::Unknown) | None => {}
            }
        }

        if result.is_empty() {
            models::Regions::Unknown
        } else {
            models::Regions::List(result.into_iter().collect())
        }
    }
}

#[async_trait]
impl runners::WikidataWorker for CondensingWorker {
    type Output = CondensingCollector;

    async fn process(
        &mut self,
        _msg: &str,
        entity: Entity,
        _tx: parallel::Sender<Self::Output>,
    ) -> Result<(), errors::ProcessingError> {
        match entity {
            Entity::Item(item) => {
                let wiki_id = WikiId::from(&item.id);

                // Gather all products
                if self.sources.is_product(&item) {
                    let categories = Self::extract_wikidata_categories(&item);
                    if !categories.is_empty() || !Self::has_categories(&item, ignored::ALL) {
                        let product_id = wiki_id.into_product_id();
                        let product = models::Product {
                            db_id: product_id.to_db_id(),
                            id: product_id.clone(),
                            gtins: models::Gtin::convert(item.get_gtins())?,
                            names: item
                                .get_labels()
                                .into_iter()
                                .map(|label| models::Text::new_wikidata(label.to_string()))
                                .collect(),
                            descriptions: item
                                .descriptions
                                .get(LANG_EN)
                                .map(|desc| vec![models::Text::new_wikidata(desc.value.clone())])
                                .unwrap_or_default(),
                            images: item
                                .get_images()
                                .unwrap_or_default()
                                .iter()
                                .map(|i| models::Image::new_wikidata(i.clone()))
                                .collect(),
                            follows: item
                                .get_follows()?
                                .unwrap_or_default()
                                .iter()
                                .map(convert::to_product_id)
                                .collect(),
                            followed_by: item
                                .get_followed_by()?
                                .unwrap_or_default()
                                .iter()
                                .map(convert::to_product_id)
                                .collect(),
                            regions: models::Regions::default(),
                            certifications: models::Certifications::default(),
                            sustainity_score: models::SustainityScore::default(),
                        };

                        self.collector.add_product(product_id.clone(), product);
                        self.collector.link_product_to_organisations(
                            product_id.clone(),
                            &item
                                .get_manufacturer_ids()?
                                .unwrap_or_default()
                                .iter()
                                .map(convert::to_org_id)
                                .collect::<Vec<_>>(),
                        );
                        self.collector.link_product_to_categories(&product_id, &categories);
                    }
                }

                // Collect all organisations
                if self.sources.is_organisation(&item) {
                    let organisation_id = wiki_id.into_organisation_id();
                    let organisation = models::Organisation {
                        db_id: format!("organisations/{}", organisation_id.to_string()),
                        id: wiki_id.into_organisation_id(),
                        vat_ids: models::VatId::convert(item.get_eu_vat_numbers())?,
                        names: item
                            .get_labels()
                            .into_iter()
                            .map(|label| models::Text::new_wikidata(label.to_string()))
                            .collect(),
                        descriptions: item
                            .descriptions
                            .get(LANG_EN)
                            .map(|desc| vec![models::Text::new_wikidata(desc.value.clone())])
                            .unwrap_or_default(),
                        images: item
                            .get_logo_images()
                            .unwrap_or_default()
                            .iter()
                            .map(|i| models::Image::new_wikidata(i.clone()))
                            .collect(),
                        websites: item
                            .get_official_websites()
                            .unwrap_or_default()
                            .into_iter()
                            .collect(),
                        certifications: models::Certifications::default(),
                    };
                    self.collector.add_organisation(organisation_id, organisation);
                }
            }
            Entity::Property(_property) => (),
        }
        Ok(())
    }

    async fn finish(
        self,
        tx: parallel::Sender<Self::Output>,
    ) -> Result<(), errors::ProcessingError> {
        tx.send(self.collector).await;
        Ok(())
    }
}

#[async_trait]
impl runners::OpenFoodFactsWorker for CondensingWorker {
    type Output = CondensingCollector;

    async fn process(
        &mut self,
        record: open_food_facts::data::Record,
        _tx: parallel::Sender<Self::Output>,
    ) -> Result<(), errors::ProcessingError> {
        // Some products have very long bar code.
        // Those are probably some internal bar codes, not GTINs.
        // Let's ignore them for now.
        if let Ok(gtin) = models::Gtin::try_from(&record.code) {
            let product_id = models::ProductId::from(gtin.clone());
            let product = models::Product {
                db_id: product_id.to_db_id(),
                id: product_id.clone(),
                gtins: [gtin].into(),
                names: vec![models::Text::new_open_food_facts(record.product_name.clone())],
                descriptions: Vec::default(),
                images: [models::Image::new_open_food_facts(record.image_small_url.clone())].into(),
                follows: HashSet::default(),
                followed_by: HashSet::default(),
                regions: models::Regions::default(),
                certifications: models::Certifications::default(),
                sustainity_score: models::SustainityScore::default(),
            };

            let categories = Self::extract_open_food_facts_categories(&record);
            let sell_regions =
                Self::extract_open_food_facts_sell_regions(&record, &self.sources.off);

            self.collector.add_product(product_id.clone(), product);
            self.collector.link_product_to_categories(&product_id, &categories);
            self.collector.link_product_to_sell_regions(&product_id, sell_regions);

            let name = utils::disambiguate_name(&record.brand_owner);
            if let Some(wiki_id) = self.sources.matches.name_to_wiki(&name) {
                self.collector
                    .link_product_to_organisations(product_id, &[wiki_id.into_organisation_id()]);
            } else {
                let mut matches = HashSet::<WikiId>::new();
                for name in record.extract_labels() {
                    let name = utils::disambiguate_name(&name);
                    if let Some(id) = self.sources.matches.name_to_wiki(&name) {
                        matches.insert(*id);
                    }
                }
                if matches.len() == 1 {
                    if let Some(wiki_id) = matches.iter().next() {
                        self.collector.link_product_to_organisations(
                            product_id,
                            &[wiki_id.into_organisation_id()],
                        );
                    }
                }
            }
        }
        Ok(())
    }

    async fn finish(
        self,
        tx: parallel::Sender<Self::Output>,
    ) -> Result<(), errors::ProcessingError> {
        tx.send(self.collector).await;
        Ok(())
    }
}

#[async_trait]
impl runners::EuEcolabelWorker for CondensingWorker {
    type Output = CondensingCollector;

    async fn process(
        &mut self,
        record: eu_ecolabel::data::Record,
        _tx: parallel::Sender<Self::Output>,
    ) -> Result<(), errors::ProcessingError> {
        if let Some(vat_number) = record.vat_number {
            let vat_number = models::VatId::try_from(&vat_number)?;
            let (organisation_id, match_accuracy): (models::OrganisationId, f64) =
                if let Some(wiki_match) = self.sources.eu_ecolabel.vat_to_wiki(&vat_number) {
                    (convert::to_org_id(&wiki_match.wiki_id), wiki_match.match_accuracy)
                } else {
                    (vat_number.clone().into(), 1.0)
                };

            let organisation = models::Organisation {
                db_id: organisation_id.to_db_id(),
                id: organisation_id.clone(),
                vat_ids: [vat_number].into(),
                names: vec![models::Text::new_eu_ecolabel(record.product_or_service_name.clone())],
                descriptions: Vec::default(),
                images: Vec::default(),
                websites: HashSet::default(),
                certifications: models::Certifications::new_with_eu_ecolabel(match_accuracy),
            };

            self.collector.add_organisation(organisation_id, organisation);

            if let Some(
                eu_ecolabel::data::Code::Ean13(code) | eu_ecolabel::data::Code::Gtin14(code),
            ) = record.code
            {
                let gtin = models::Gtin::try_from(code)?;
                let product_id = models::ProductId::from(gtin.clone());

                let product = models::Product {
                    db_id: product_id.to_db_id(),
                    id: product_id.clone(),
                    gtins: [gtin].into(),
                    names: vec![models::Text::new_eu_ecolabel(record.product_or_service_name)],
                    descriptions: Vec::default(),
                    images: Vec::default(),
                    follows: HashSet::default(),
                    followed_by: HashSet::default(),
                    regions: models::Regions::default(),
                    certifications: models::Certifications::new_with_eu_ecolabel(match_accuracy),
                    sustainity_score: models::SustainityScore::default(),
                };

                self.collector.add_product(product_id, product);
            }
        }
        Ok(())
    }

    async fn finish(
        self,
        tx: parallel::Sender<Self::Output>,
    ) -> Result<(), errors::ProcessingError> {
        tx.send(self.collector).await;
        Ok(())
    }
}

#[derive(Clone)]
pub struct CondensingStash {
    /// Collected data.
    collector: CondensingCollector,

    /// Additional data sources.
    sources: Arc<sources::FullSources>,

    /// Configuration.
    config: config::CondensationConfig,
}

impl CondensingStash {
    #[must_use]
    pub fn new(sources: Arc<sources::FullSources>, config: config::CondensationConfig) -> Self {
        Self { collector: CondensingCollector::default(), sources, config }
    }

    /// Extracts keywords for DB text search from passed texts.
    fn extract_keywords(texts: &[models::Text]) -> HashSet<String> {
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
    fn prepare_organisations(&mut self) -> (Vec<models::Organisation>, usize) {
        log::info!("Preparing organisations");

        let mut num_wiki_organisations: usize = 0;
        for organisation in self.collector.organisations.values_mut() {
            let domains = utils::extract_domains_from_urls(&organisation.websites);

            let bcorp_cert = self.sources.bcorp.get_cert_from_domains(&domains);
            let (tco_cert, fti_cert) = {
                match &organisation.id {
                    models::OrganisationId::Wiki(wiki_id) => {
                        let tco_cert = self.sources.tco.get_company_cert(&wiki_id.into());
                        let fti_cert = self.sources.fti.get_cert(&wiki_id.into());
                        num_wiki_organisations += 1;
                        (tco_cert, fti_cert)
                    }
                    models::OrganisationId::Vat(_) => (None, None),
                }
            };

            organisation.certifications.inherit(&models::Certifications {
                bcorp: bcorp_cert,
                tco: tco_cert,
                eu_ecolabel: None, // not updated
                fti: fti_cert,
            });
        }

        (self.collector.organisations.values().cloned().collect(), num_wiki_organisations)
    }

    /// Prepares organsation keywords data.
    ///
    /// This data is needed to implement an efficient text search index.
    /// Data is composed from keyword vertex collection and edge collection connecting them to organisations.
    fn prepare_organisation_keywords(&self) -> (Vec<models::Keyword>, Vec<models::Edge>) {
        log::info!("Preparing organisation keywords");

        let mut keywords = HashMap::<String, HashSet<models::OrganisationId>>::new();
        for organisation in self.collector.organisations.values() {
            for keyword in Self::extract_keywords(&organisation.names) {
                keywords
                    .entry(keyword)
                    .and_modify(|ids| {
                        ids.insert(organisation.id.clone());
                    })
                    .or_insert_with(|| [organisation.id.clone()].into());
            }
        }

        let mut organisation_keywords = Vec::<models::Keyword>::with_capacity(keywords.len());
        let mut organisation_keyword_edges = Vec::<models::Edge>::new();
        for (keyword, organisation_ids) in keywords {
            let digest = md5::compute(keyword.as_bytes());
            let db_id = format!("organisation_keywords/{digest:x}");
            organisation_keywords
                .push(models::Keyword { db_id: db_id.clone(), keyword: keyword.clone() });
            for organisation_id in organisation_ids {
                organisation_keyword_edges
                    .push(models::Edge { from: db_id.clone(), to: organisation_id.to_db_id() });
            }
        }

        (organisation_keywords, organisation_keyword_edges)
    }

    /// Prepares product data.
    ///
    /// - fills left-over certifications
    /// - converts into a vector
    fn prepare_products(&mut self) -> Vec<models::Product> {
        log::info!("Preparing products");

        for product in self.collector.products.values_mut() {
            // Assign certifications to products
            if let Some(manufacturer_ids) = self.collector.product_to_organisations.get(&product.id)
            {
                for manufacturer_id in manufacturer_ids {
                    if let Some(organisation) = self.collector.organisations.get(manufacturer_id) {
                        product.certifications.inherit(&organisation.certifications);
                    }
                    // TODO: There are many organisations that cannot be found.
                    //       It seems like all of them are bugs in Wikidata.
                    //       Make sure all organisations are found.
                }
            }

            // Assign region codes to products
            if let Some(regions) = self.collector.product_to_regions.get(&product.id) {
                product.regions = regions.clone();
            }

            // Calculate product Sustainity score
            {
                let has_producer = self
                    .collector
                    .product_to_organisations
                    .get(&product.id)
                    .map_or(false, |o| !o.is_empty());
                let categories = self.collector.product_to_categories.get(&product.id);
                product.sustainity_score =
                    crate::score::calculate(product, has_producer, categories);
            }
        }

        self.collector.products.values().cloned().collect()
    }

    /// Prepares product keywords data.
    ///
    /// This data is needed to implement an efficient text search index.
    /// Data is composed from keyword vertex collection and edge collection connecting them to products.
    fn prepare_product_keywords(&self) -> (Vec<models::Keyword>, Vec<models::Edge>) {
        log::info!("Preparing product keywords");

        let mut keywords = HashMap::<String, HashSet<models::ProductId>>::new();
        for product in self.collector.products.values() {
            for keyword in Self::extract_keywords(&product.names) {
                keywords
                    .entry(keyword)
                    .and_modify(|ids| {
                        ids.insert(product.id.clone());
                    })
                    .or_insert_with(|| [product.id.clone()].into());
            }
        }

        let mut product_keywords = Vec::<models::Keyword>::with_capacity(keywords.len());
        let mut product_keyword_edges = Vec::<models::Edge>::new();
        for (keyword, product_ids) in keywords {
            let digest = md5::compute(keyword.as_bytes());
            let db_id = format!("product_keywords/{digest:x}");
            product_keywords
                .push(models::Keyword { db_id: db_id.clone(), keyword: keyword.clone() });
            for product_id in product_ids {
                product_keyword_edges
                    .push(models::Edge { from: db_id.clone(), to: product_id.to_db_id() });
            }
        }

        (product_keywords, product_keyword_edges)
    }

    /// Prepares GTIN data.
    ///
    /// This data is needed to implement an efficient GTIN search index.
    /// Data is composed from GTIN vertex collection and edge collection connecting them to products.
    fn prepare_gtins(&self) -> (Vec<models::IdEntry>, Vec<models::Edge>) {
        log::info!("Preparing GTINs");
        let mut gtins = Vec::<models::IdEntry>::new();
        let mut gtin_edges = Vec::<models::Edge>::new();
        for product in self.collector.products.values() {
            for gtin in &product.gtins {
                let db_id = gtin.to_db_id();
                gtins.push(models::IdEntry { db_id: db_id.clone() });
                gtin_edges.push(models::Edge { from: db_id.clone(), to: product.id.to_db_id() });
            }
        }

        (gtins, gtin_edges)
    }

    /// Prepares category data.
    ///
    /// This data is needed to implement an efficient alternative product search index.
    /// Data is composed from category vertex collection and edge collection connecting them to products.
    fn prepare_categories(&self) -> (Vec<models::IdEntry>, Vec<models::Edge>) {
        log::info!("Preparing categories");
        let mut categories = Vec::<models::IdEntry>::new();
        let mut category_edges = Vec::<models::Edge>::new();
        for (category, product_ids) in &self.collector.category_to_products {
            if product_ids.len() < MAX_CATEGORY_PRODUCT_NUM {
                let db_id = format!("categories/{category}");
                categories.push(models::IdEntry { db_id: db_id.clone() });
                for product_id in product_ids {
                    category_edges
                        .push(models::Edge { from: db_id.clone(), to: product_id.to_db_id() });
                }
            } else {
                log::info!(
                    " - skipping category `{}` with {} products",
                    category,
                    product_ids.len()
                );
            }
        }

        (categories, category_edges)
    }

    /// Prepares manufacturing data.
    ///
    /// Data is domeposed from edges connecting produects to their manufacturers.
    fn prepare_manufacturing(&self) -> Vec<models::Edge> {
        log::info!("Preparing manufacturing");
        let mut manufacturing_edges = Vec::<models::Edge>::new();
        for (product_id, organisation_ids) in &self.collector.product_to_organisations {
            for organisation_id in organisation_ids {
                manufacturing_edges.push(models::Edge {
                    from: organisation_id.to_db_id(),
                    to: product_id.to_db_id(),
                });
            }
        }
        manufacturing_edges
    }

    /// Prepares presentations for the Library paths.
    fn prepare_presentations(&self) -> Vec<models::Presentation> {
        log::info!("Preparing presentations");
        vec![self.sources.fti.prepare_presentation()]
    }

    /// Saves organisations.
    fn save_organisations(
        &self,
        organisations: (Vec<models::Organisation>, usize),
    ) -> Result<(), errors::ProcessingError> {
        let (mut organisations, num_wiki_organisations) = organisations;
        log::info!(
            "Saving {} organisations. ({} come from Wikidata)",
            organisations.len(),
            num_wiki_organisations
        );
        organisations.sort_by(|a, b| a.id.cmp(&b.id));
        serde_jsonlines::write_json_lines(&self.config.target.organisations_path, &organisations)?;
        Ok(())
    }

    /// Saves organisation keywords.
    fn save_organisation_keywords(
        &self,
        organisation_keywords: (Vec<models::Keyword>, Vec<models::Edge>),
    ) -> Result<(), errors::ProcessingError> {
        let (mut organisation_keywords, mut organisation_keyword_edges) = organisation_keywords;

        log::info!("Saving {} organisation keywords", organisation_keywords.len());
        organisation_keywords.sort();
        serde_jsonlines::write_json_lines(
            &self.config.target.organisation_keywords_path,
            &organisation_keywords,
        )?;

        log::info!("Saving {} organisation keyword edges", organisation_keyword_edges.len());
        organisation_keyword_edges.sort();
        serde_jsonlines::write_json_lines(
            &self.config.target.organisation_keyword_edges_path,
            &organisation_keyword_edges,
        )?;

        Ok(())
    }

    /// Saves products.
    fn save_products(
        &self,
        mut products: Vec<models::Product>,
    ) -> Result<(), errors::ProcessingError> {
        log::info!("Saving {} products.", products.len());
        products.sort_by(|a, b| a.id.cmp(&b.id));
        serde_jsonlines::write_json_lines(&self.config.target.products_path, &products)?;
        Ok(())
    }

    /// Saves product keywords.
    fn save_product_keywords(
        &self,
        product_keywords: (Vec<models::Keyword>, Vec<models::Edge>),
    ) -> Result<(), errors::ProcessingError> {
        let (mut product_keywords, mut product_keyword_edges) = product_keywords;

        log::info!("Saving {} product keywords", product_keywords.len());
        product_keywords.sort();
        serde_jsonlines::write_json_lines(
            &self.config.target.product_keywords_path,
            &product_keywords,
        )?;

        log::info!("Saving {} product keyword edges", product_keyword_edges.len());
        product_keyword_edges.sort();
        serde_jsonlines::write_json_lines(
            &self.config.target.product_keyword_edges_path,
            &product_keyword_edges,
        )?;

        Ok(())
    }

    /// Saves GTINs.
    fn save_gtins(
        &self,
        gtins: (Vec<models::IdEntry>, Vec<models::Edge>),
    ) -> Result<(), errors::ProcessingError> {
        let (mut gtins, mut gtin_edges) = gtins;

        log::info!("Saving {} product GTINs", gtins.len());
        gtins.sort();
        serde_jsonlines::write_json_lines(&self.config.target.gtins_path, &gtins)?;

        log::info!("Saving {} product GTIN edges", gtin_edges.len());
        gtin_edges.sort();
        serde_jsonlines::write_json_lines(&self.config.target.gtin_edges_path, &gtin_edges)?;

        Ok(())
    }

    /// Saves categories.
    fn save_categories(
        &self,
        categories: (Vec<models::IdEntry>, Vec<models::Edge>),
    ) -> Result<(), errors::ProcessingError> {
        let (mut categories, mut category_edges) = categories;

        log::info!("Saving {} product categories", categories.len());
        categories.sort();
        serde_jsonlines::write_json_lines(&self.config.target.categories_path, &categories)?;

        log::info!("Saving {} product category edges", category_edges.len());
        category_edges.sort();
        serde_jsonlines::write_json_lines(
            &self.config.target.category_edges_path,
            &category_edges,
        )?;

        Ok(())
    }

    /// Saves product to organisation edges.
    fn save_manufacturing(
        &self,
        mut manufacturing_edges: Vec<models::Edge>,
    ) -> Result<(), errors::ProcessingError> {
        log::info!("Saving {} manufacturing edges", manufacturing_edges.len());
        manufacturing_edges.sort();
        serde_jsonlines::write_json_lines(
            &self.config.target.manufacturing_edges_path,
            &manufacturing_edges,
        )?;
        Ok(())
    }

    /// Saves presentations
    fn save_presentations(
        &self,
        mut presentations: Vec<models::Presentation>,
    ) -> Result<(), errors::ProcessingError> {
        log::info!("Saving {} presentations", presentations.len(),);
        presentations.sort_by(|a, b| a.id.cmp(&b.id));
        serde_jsonlines::write_json_lines(&self.config.target.presentations_path, &presentations)?;
        Ok(())
    }
}

#[async_trait]
impl runners::Stash for CondensingStash {
    type Input = CondensingCollector;

    fn stash(&mut self, input: Self::Input) -> Result<(), errors::ProcessingError> {
        log::info!("Merging collectors");
        self.collector.merge(input);
        Ok(())
    }

    fn finish(mut self) -> Result<(), errors::ProcessingError> {
        log::info!("Saving");

        let organisations = self.prepare_organisations();
        self.save_organisations(organisations)?;

        let organisation_keywords = self.prepare_organisation_keywords();
        self.save_organisation_keywords(organisation_keywords)?;

        let products = self.prepare_products();
        self.save_products(products)?;

        let product_keywords = self.prepare_product_keywords();
        self.save_product_keywords(product_keywords)?;

        let gtins = self.prepare_gtins();
        self.save_gtins(gtins)?;

        let categories = self.prepare_categories();
        self.save_categories(categories)?;

        let manufacturing_edges = self.prepare_manufacturing();
        self.save_manufacturing(manufacturing_edges)?;

        let presentations = self.prepare_presentations();
        self.save_presentations(presentations)?;

        log::info!("Condensation finished");

        Ok(())
    }
}

pub struct CondensingRunner;

impl CondensingRunner {
    pub fn run(config: &config::CondensationConfig) -> Result<(), errors::ProcessingError> {
        let sources = Arc::new(sources::FullSources::load(&config.into())?);

        let worker = CondensingWorker::new(sources.clone());
        let stash = CondensingStash::new(sources, config.clone());

        let flow = parallel::Flow::new();
        runners::FullRunner::flow(flow, config, &worker, stash)?.join();

        Ok(())
    }
}
