// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;

use transpaer_collecting::{bcorp, eu_ecolabel, fashion_transparency_index, open_food_facts, tco};
use transpaer_models::{
    gather as models,
    ids::WikiId,
    utils::{extract_domain_from_url, extract_domains_from_urls},
};
use transpaer_schema as schema;
use transpaer_wikidata::{
    data::{Entity, Item},
    errors::ParseIdError,
};

use crate::{advisors, config, errors, parallel, runners, utils, wikidata::ItemExt};

const LANG_EN: &str = "en";

/// Holds all the supplementary source data.
pub struct CondensationSources {
    /// Wikidata data.
    pub wikidata: advisors::WikidataAdvisor,

    /// Names (company, brand, etc...) matched to Wikidata items representing them.
    pub matches: advisors::TranspaerMatchesAdvisor,

    /// B-Corp data.
    pub bcorp: advisors::BCorpAdvisor,

    /// EU Ecolabel data.
    pub eu_ecolabel: advisors::EuEcolabelAdvisor,

    /// TCO data.
    pub tco: advisors::TcoAdvisor,

    /// Fashion Transparency Index data.
    pub fti: advisors::FashionTransparencyIndexAdvisor,

    /// Open Food Facts advisor.
    pub off: advisors::OpenFoodFactsAdvisor,
}

impl CondensationSources {
    #[allow(clippy::unused_self)]
    #[must_use]
    pub fn is_product(&self, item: &transpaer_wikidata::data::Item) -> bool {
        item.has_manufacturer() || item.has_gtin()
    }

    #[must_use]
    pub fn is_organisation(&self, item: &transpaer_wikidata::data::Item) -> bool {
        if self.is_product(item) {
            return false;
        }

        if item.is_organisation() {
            return true;
        }

        if self.wikidata.has_manufacturer_id(&item.id) {
            return true;
        }

        if self.fti.has_company(&item.id) || self.tco.has_company(&item.id) {
            return true;
        }

        if let Some(websites) = item.get_official_websites() {
            let domains = extract_domains_from_urls(&websites);
            if self.bcorp.has_domains(&domains) {
                return true;
            }
        }

        false
    }
}

impl CondensationSources {
    /// Constructs a new `CondensationSources`.
    fn load(config: &config::CondensationConfig) -> Result<Self, errors::ProcessingError> {
        let wikidata = advisors::WikidataAdvisor::load(
            &config.cache.wikidata_cache_path,
            &config.meta.wikidata_regions_path,
            &config.meta.wikidata_categories_path,
        )?;
        let matches = advisors::TranspaerMatchesAdvisor::load(&config.meta.match_path)?;
        let bcorp = advisors::BCorpAdvisor::load(
            &config.origin.bcorp_path,
            &config.meta.bcorp_regions_path,
        )?;
        let eu_ecolabel = advisors::EuEcolabelAdvisor::load(
            &config.origin.eu_ecolabel_path,
            &config.meta.match_path,
        )?;
        let tco = advisors::TcoAdvisor::load(&config.support.tco_path)?;
        let fti = advisors::FashionTransparencyIndexAdvisor::load(
            &config.support.fashion_transparency_index_path,
        )?;
        let off = advisors::OpenFoodFactsAdvisor::load(
            &config.meta.open_food_facts_regions_path,
            &config.meta.open_food_facts_categories_path,
        )?;

        Ok(Self { wikidata, matches, bcorp, eu_ecolabel, tco, fti, off })
    }
}

fn prepare_meta(variant: schema::ProviderVariant) -> schema::Meta {
    schema::Meta {
        version: "0.0.0".to_owned(),
        variant,
        authors: vec!["Transpaer Development Team".to_owned()],
        title: String::new(),
        description: Some("Data prepared by the Transpaer Development Team".to_owned()),
        creation_timestamp: Some(schema::chrono::Utc::now()),
        valid_from: None,
        valid_to: None,
    }
}

fn merge_catalog_producers(p1: &mut schema::CatalogProducer, p2: &schema::CatalogProducer) {
    let r: schema::CatalogProducer = p1.merge(p2);
    *p1 = r;
}

fn merge_review_producers(p1: &mut schema::ReviewProducer, p2: &schema::ReviewProducer) {
    let r: schema::ReviewProducer = p1.merge(p2);
    *p1 = r;
}

pub trait Collector: Clone + Default + Send {
    type About: Clone + Send;

    fn build_substrate(self, about: Self::About) -> schema::Substrate;
    fn merge(&mut self, other: Self) -> Result<(), errors::CondensationError>;
}

/// Data storage for gathered data from a cataloger.
///
/// Allows merging different instances.
#[derive(Debug, Clone, Default)]
pub struct CatalogerCollector {
    producers: HashMap<String, schema::CatalogProducer>,
    products: Vec<schema::CatalogProduct>,
}

impl Collector for CatalogerCollector {
    type About = schema::AboutCataloger;

    fn build_substrate(mut self, about: Self::About) -> schema::Substrate {
        let mut producers: Vec<schema::CatalogProducer> = self.producers.into_values().collect();
        producers.sort_by(|a, b| a.id.cmp(&b.id));
        self.products.sort_by(|a, b| a.id.cmp(&b.id));

        schema::Substrate {
            meta: prepare_meta(schema::ProviderVariant::Cataloger),
            data: schema::Data::Cataloger(schema::CatalogerData {
                cataloger: about,
                producers,
                products: self.products,
            }),
        }
    }

    fn merge(&mut self, other: Self) -> Result<(), errors::CondensationError> {
        utils::merge_hashmaps_with(&mut self.producers, other.producers, merge_catalog_producers);
        merge::vec::append(&mut self.products, other.products);
        Ok(())
    }
}

impl CatalogerCollector {
    pub fn insert_producer(&mut self, producer: schema::CatalogProducer) {
        match self.producers.entry(producer.id.clone()) {
            Entry::Occupied(mut entry) => {
                let _ = entry.insert(entry.get().merge(&producer));
            }
            Entry::Vacant(entry) => {
                let _ = entry.insert(producer);
            }
        }
    }

    pub fn add_product(&mut self, product: schema::CatalogProduct) {
        self.products.push(product);
    }
}

/// Data storage for gathered data from a reviewer.
///
/// Allows merging different instances.
#[derive(Debug, Clone, Default)]
pub struct ReviewerCollector {
    producers: HashMap<String, schema::ReviewProducer>,
    products: Vec<schema::ReviewProduct>,
}

impl Collector for ReviewerCollector {
    type About = schema::AboutReviewer;

    fn build_substrate(mut self, about: Self::About) -> schema::Substrate {
        let mut producers: Vec<schema::ReviewProducer> = self.producers.into_values().collect();
        producers.sort_by(|a, b| a.id.cmp(&b.id));
        self.products.sort_by(|a, b| a.id.cmp(&b.id));

        schema::Substrate {
            meta: prepare_meta(schema::ProviderVariant::Reviewer),
            data: schema::Data::Reviewer(schema::ReviewerData {
                reviewer: about,
                producers,
                products: self.products,
            }),
        }
    }

    fn merge(&mut self, other: Self) -> Result<(), errors::CondensationError> {
        utils::merge_hashmaps_with(&mut self.producers, other.producers, merge_review_producers);
        merge::vec::append(&mut self.products, other.products);
        Ok(())
    }
}

impl ReviewerCollector {
    pub fn insert_producer(&mut self, producer: schema::ReviewProducer) {
        match self.producers.entry(producer.id.clone()) {
            Entry::Occupied(mut entry) => {
                let _ = entry.insert(entry.get().merge(&producer));
            }
            Entry::Vacant(entry) => {
                let _ = entry.insert(producer);
            }
        }
    }

    pub fn add_product(&mut self, product: schema::ReviewProduct) {
        self.products.push(product);
    }
}

pub trait About {
    type Collector: Collector;

    fn name() -> &'static str;
    fn variant() -> schema::SubstrateExtension;
    fn build() -> <<Self as About>::Collector as Collector>::About;
}

#[derive(Clone)]
struct AboutBCorp;

impl About for AboutBCorp {
    type Collector = ReviewerCollector;

    fn name() -> &'static str {
        "bcorp"
    }

    fn variant() -> schema::SubstrateExtension {
        schema::SubstrateExtension::JsonLines
    }

    fn build() -> schema::AboutReviewer {
        schema::AboutReviewer {
            id: "bcorp".to_owned(),
            name: "BCorp".to_owned(),
            description: "Data from the BCorp prepared by the Transpaer Team".to_owned(),
            website: "https://www.bcorporation.net".to_owned(),
            reviews: Some(schema::AboutReview::Certification(schema::AboutCertification(
                serde_json::Map::new(),
            ))),
        }
    }
}

#[derive(Clone, Default)]
struct AboutEu;

impl About for AboutEu {
    type Collector = ReviewerCollector;

    fn name() -> &'static str {
        "eu_ecolabel"
    }

    fn variant() -> schema::SubstrateExtension {
        schema::SubstrateExtension::JsonLines
    }

    fn build() -> schema::AboutReviewer {
        schema::AboutReviewer {
            id: "eu_ecolabel".to_owned(),
            name: "EU EcoLabel".to_owned(),
            description: "Data from the EU EcoLabel prepared by the Transpaer Team".to_owned(),
            website: "https://environment.ec.europa.eu".to_owned(),
            reviews: Some(schema::AboutReview::Certification(schema::AboutCertification(
                serde_json::Map::new(),
            ))),
        }
    }
}

#[derive(Clone)]
struct AboutFti;

impl About for AboutFti {
    type Collector = ReviewerCollector;

    fn name() -> &'static str {
        "fti"
    }

    fn variant() -> schema::SubstrateExtension {
        schema::SubstrateExtension::JsonLines
    }

    fn build() -> schema::AboutReviewer {
        schema::AboutReviewer {
            id: "fti".to_owned(),
            name: "Fashion Transparency Index".to_owned(),
            description: "Data from the Fashion Transparency Index prepared by the Transpaer Team"
                .to_owned(),
            website: "https://www.fashionrevolution.org".to_owned(),
            reviews: Some(schema::AboutReview::ScoreReview(schema::AboutScoreReview {
                min: 0,
                max: 100,
                div: 1,
            })),
        }
    }
}

#[derive(Clone, Default)]
struct AboutOff;

impl About for AboutOff {
    type Collector = CatalogerCollector;

    fn name() -> &'static str {
        "open_food_facts"
    }

    fn variant() -> schema::SubstrateExtension {
        schema::SubstrateExtension::JsonLines
    }

    fn build() -> schema::AboutCataloger {
        schema::AboutCataloger {
            id: "open_food_facts".to_owned(),
            name: "Open Food Facts".to_owned(),
            description: Some(
                "Data from the Open Food Facts prepared by the Transpaer Team".to_owned(),
            ),
            variant: schema::CatalogVariant::Database,
            website: "https://world.openfoodfacts.org".to_owned(),
        }
    }
}

#[derive(Clone)]
struct AboutTco;

impl About for AboutTco {
    type Collector = ReviewerCollector;

    fn name() -> &'static str {
        "tco"
    }

    fn variant() -> schema::SubstrateExtension {
        schema::SubstrateExtension::JsonLines
    }

    fn build() -> schema::AboutReviewer {
        schema::AboutReviewer {
            id: "tco".to_owned(),
            name: "TCO".to_owned(),
            description: "Data from the TCO prepared by the Transpaer Team".to_owned(),
            website: "https://tcocertified.com".to_owned(),
            reviews: Some(schema::AboutReview::Certification(schema::AboutCertification(
                serde_json::Map::new(),
            ))),
        }
    }
}

#[derive(Clone, Default)]
struct AboutWiki;

impl About for AboutWiki {
    type Collector = CatalogerCollector;

    fn name() -> &'static str {
        "wikidata"
    }

    fn variant() -> schema::SubstrateExtension {
        schema::SubstrateExtension::JsonLines
    }

    fn build() -> schema::AboutCataloger {
        schema::AboutCataloger {
            id: "wikidata".to_owned(),
            name: "Wikidata".to_owned(),
            description: Some("Data from the Wikidata prepared by the Transpaer Team".to_owned()),
            variant: schema::CatalogVariant::Database,
            website: "https://www.wikidata.org/".to_owned(),
        }
    }
}

#[derive(Clone)]
pub struct CondensingWikidataWorker {
    sources: Arc<CondensationSources>,
    collector: CatalogerCollector,
}

impl CondensingWikidataWorker {
    #[must_use]
    pub fn new(sources: Arc<CondensationSources>) -> Self {
        Self { collector: CatalogerCollector::default(), sources }
    }

    /// Extracts categories from a Wikidata item.
    fn extract_wikidata_categories(
        &self,
        item: &Item,
    ) -> Result<Vec<String>, errors::ProcessingError> {
        let mut result = HashSet::<String>::new();

        if let Some(classes) = item.get_classes()? {
            for class in classes {
                if let Some(categories) = self.sources.wikidata.get_categories(&class) {
                    result.extend(categories.iter().cloned());
                }
            }
        }

        if let Some(classes) = item.get_superclasses()? {
            for class in classes {
                if let Some(categories) = self.sources.wikidata.get_categories(&class) {
                    result.extend(categories.iter().cloned());
                }
            }
        }

        Ok(result.into_iter().collect())
    }

    /// Extracts countries from a Wikidata item.
    fn extract_wikidata_regions(
        &self,
        item: &Item,
    ) -> Result<Option<schema::RegionList>, ParseIdError> {
        let mut result = HashSet::<isocountry::CountryCode>::new();
        let countries = item.get_countries()?;
        for country_id in countries.unwrap_or_default() {
            match self.sources.wikidata.get_regions(&country_id) {
                Some(models::Regions::List(list)) => result.extend(list.iter()),
                Some(models::Regions::Unknown | models::Regions::World) | None => {}
            }
        }

        if result.is_empty() {
            Ok(None)
        } else {
            Ok(Some(schema::RegionList(
                result.into_iter().map(|code| code.alpha3().to_owned()).collect(),
            )))
        }
    }
}

#[async_trait]
impl runners::WikidataWorker for CondensingWikidataWorker {
    type Output = CatalogerCollector;

    async fn process(
        &mut self,
        _msg: &str,
        entity: Entity,
        _tx: parallel::Sender<Self::Output>,
    ) -> Result<(), errors::ProcessingError> {
        match entity {
            Entity::Item(item) => {
                // Gather all products
                if self.sources.is_product(&item) {
                    let categories = self.extract_wikidata_categories(&item)?;
                    let regions = self.extract_wikidata_regions(&item)?;
                    let product = schema::CatalogProduct {
                        id: item.id.to_id(),
                        ids: schema::ProductIds {
                            ean: None,
                            gtin: item.get_gtins(),
                            wiki: Some(vec![item.id.to_id()]),
                        },
                        names: item.get_labels().into_iter().map(ToString::to_string).collect(),
                        description: item
                            .descriptions
                            .get(LANG_EN)
                            .map(|label| label.value.clone()),
                        images: item.get_images().unwrap_or_default(),
                        categorisation: Some(schema::ProductCategorisation {
                            categories: categories
                                .into_iter()
                                .map(schema::ProductCategory)
                                .collect(),
                        }),
                        origins: Some(schema::ProductOrigins {
                            producer_ids: item
                                .get_manufacturer_ids()?
                                .unwrap_or_default()
                                .iter()
                                .map(transpaer_collecting::data::WikiId::to_id)
                                .collect(),
                            regions,
                        }),
                        availability: None,
                        related: Some(schema::RelatedProducts {
                            preceded_by: Some(
                                item.get_follows()?
                                    .unwrap_or_default()
                                    .iter()
                                    .map(transpaer_collecting::data::WikiId::to_id)
                                    .collect(),
                            ),
                            followed_by: Some(
                                item.get_followed_by()?
                                    .unwrap_or_default()
                                    .iter()
                                    .map(transpaer_collecting::data::WikiId::to_id)
                                    .collect(),
                            ),
                        }),
                        shopping: item.get_asins().map(|asins| {
                            schema::Shopping(
                                asins
                                    .iter()
                                    .map(|asin| schema::ShoppingEntry {
                                        id: asin.clone(),
                                        description: String::new(),
                                        shop: schema::VerifiedShop::Amazon,
                                    })
                                    .collect(),
                            )
                        }),
                    };

                    self.collector.add_product(product);
                }

                // Collect all organisations
                if self.sources.is_organisation(&item) {
                    let regions = self.extract_wikidata_regions(&item)?;
                    let producer = schema::CatalogProducer {
                        id: item.id.to_id(),
                        ids: schema::ProducerIds {
                            vat: item.get_eu_vat_numbers(),
                            wiki: Some(vec![item.id.to_id()]),
                            domains: item.extract_domains().map(|c| c.into_iter().collect()),
                        },
                        names: item.get_labels().into_iter().map(ToString::to_string).collect(),
                        description: item
                            .descriptions
                            .get(LANG_EN)
                            .map(|label| label.value.clone()),
                        images: item.get_logo_images().unwrap_or_default(),
                        websites: item.get_official_websites().unwrap_or_default(),
                        origins: Some(schema::ProducerOrigins { regions }),
                    };
                    self.collector.insert_producer(producer);
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

#[derive(Clone)]
pub struct CondensingOpenFoodFactsWorker {
    sources: Arc<CondensationSources>,
    collector: CatalogerCollector,
}

impl CondensingOpenFoodFactsWorker {
    #[must_use]
    pub fn new(sources: Arc<CondensationSources>) -> Self {
        Self { collector: CatalogerCollector::default(), sources }
    }

    /// Extracts categories from a Wikidata item.
    fn extract_open_food_facts_categories(
        &self,
        record: &open_food_facts::data::Record,
    ) -> Vec<String> {
        let mut result = HashSet::<String>::new();
        for tag in record.food_groups_tags.split(',') {
            if let Some(categories) = self.sources.off.get_categories(tag) {
                result.extend(categories.iter().cloned());
            }
        }
        result.into_iter().collect()
    }

    /// Extracts production regions from Open Food Facts record.
    fn extract_open_food_facts_production_regions(
        &self,
        record: &open_food_facts::data::Record,
    ) -> Option<schema::RegionList> {
        let mut result = HashSet::<isocountry::CountryCode>::new();
        for tag in record.extract_sell_countries() {
            match self.sources.off.get_countries(&tag) {
                Some(models::Regions::List(list)) => result.extend(list.iter()),
                Some(models::Regions::Unknown | models::Regions::World) | None => {}
            }
        }

        if result.is_empty() {
            None
        } else {
            Some(schema::RegionList(
                result.into_iter().map(|code| code.alpha3().to_owned()).collect(),
            ))
        }
    }

    /// Extracts sell regions from Open Food Facts record.
    fn extract_open_food_facts_sell_regions(
        record: &open_food_facts::data::Record,
        off: &advisors::OpenFoodFactsAdvisor,
    ) -> schema::Regions {
        let mut result = HashSet::<isocountry::CountryCode>::new();
        for tag in record.extract_sell_countries() {
            match off.get_countries(&tag) {
                Some(models::Regions::World) => {
                    return schema::Regions::Variant(schema::RegionVariant::All)
                }
                Some(models::Regions::List(list)) => result.extend(list.iter()),
                Some(models::Regions::Unknown) | None => {}
            }
        }

        if result.is_empty() {
            schema::Regions::Variant(schema::RegionVariant::Unknown)
        } else {
            schema::Regions::List(schema::RegionList(
                result.into_iter().map(|code| code.alpha3().to_owned()).collect(),
            ))
        }
    }

    fn get_producer_id(record: &open_food_facts::data::Record) -> Option<String> {
        let id = utils::disambiguate_name(&record.brand_owner);
        if id.is_empty() {
            None
        } else {
            Some(id)
        }
    }

    fn guess_producer_wiki_id(&self, record: &open_food_facts::data::Record) -> Option<WikiId> {
        if let Some(name) = Self::get_producer_id(record) {
            if let Some(wiki_id) = self.sources.matches.name_to_wiki(&name) {
                Some(WikiId::from(*wiki_id))
            } else {
                let mut matches = HashSet::<WikiId>::new();
                for name in record.extract_brand_labels() {
                    let name = utils::disambiguate_name(&name);
                    if let Some(id) = self.sources.matches.name_to_wiki(&name) {
                        matches.insert(WikiId::from(*id));
                    }
                }
                if matches.len() == 1 {
                    return matches.iter().next().copied();
                }
                None
            }
        } else {
            None
        }
    }

    fn vec(string: &str) -> Vec<String> {
        if string.is_empty() {
            Vec::new()
        } else {
            vec![string.to_owned()]
        }
    }
}

#[async_trait]
impl runners::OpenFoodFactsWorker for CondensingOpenFoodFactsWorker {
    type Output = CatalogerCollector;

    async fn process(
        &mut self,
        record: open_food_facts::data::Record,
        _tx: parallel::Sender<Self::Output>,
    ) -> Result<(), errors::ProcessingError> {
        // Some products have very long bar code.
        // Those are probably some internal bar codes, not GTINs.
        // Let's ignore them for now.
        if let Ok(gtin) = models::Gtin::try_from(&record.code) {
            let categories = self.extract_open_food_facts_categories(&record);
            let producer_id = Self::get_producer_id(&record);

            let product = schema::CatalogProduct {
                id: gtin.to_string(),
                ids: schema::ProductIds {
                    ean: None,
                    gtin: Some(vec![gtin.to_string()]),
                    wiki: None,
                },
                names: Self::vec(&record.product_name),
                description: None,
                images: Self::vec(&record.image_small_url),
                categorisation: Some(schema::ProductCategorisation {
                    categories: categories.into_iter().map(schema::ProductCategory).collect(),
                }),
                origins: Some(schema::ProductOrigins {
                    producer_ids: producer_id.as_ref().map_or_else(Vec::new, |id| vec![id.clone()]),
                    regions: self.extract_open_food_facts_production_regions(&record),
                }),
                availability: Some(schema::ProductAvailability {
                    regions: Self::extract_open_food_facts_sell_regions(&record, &self.sources.off),
                }),
                related: None,
                shopping: None,
            };

            self.collector.add_product(product);

            if let Some(producer_id) = producer_id {
                let producer = schema::CatalogProducer {
                    id: producer_id,
                    ids: schema::ProducerIds {
                        vat: None,
                        wiki: self
                            .guess_producer_wiki_id(&record)
                            .map(|id| vec![id.to_canonical_string()]),
                        domains: None,
                    },
                    description: None,
                    images: Vec::new(),
                    names: record.extract_brand_labels(),
                    websites: Vec::new(),
                    origins: Some(schema::ProducerOrigins {
                        regions: self.extract_open_food_facts_production_regions(&record),
                    }),
                };

                self.collector.insert_producer(producer);
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
pub struct CondensingEuEcolabelWorker {
    sources: Arc<CondensationSources>,
    collector: ReviewerCollector,
}

impl CondensingEuEcolabelWorker {
    #[must_use]
    pub fn new(sources: Arc<CondensationSources>) -> Self {
        Self { collector: ReviewerCollector::default(), sources }
    }

    fn extract_region(record: &eu_ecolabel::data::Record) -> Option<schema::RegionList> {
        match isocountry::CountryCode::for_alpha2(&record.company_country) {
            Ok(code) => Some(schema::RegionList(vec![code.alpha3().to_owned()])),
            Err(err) => {
                log::warn!(
                    "EuEcoLabel country `{}` is not a valid alpha2 code: {}",
                    record.company_country,
                    err
                );
                None
            }
        }
    }
}

#[async_trait]
impl runners::EuEcolabelWorker for CondensingEuEcolabelWorker {
    type Output = ReviewerCollector;

    async fn process(
        &mut self,
        record: eu_ecolabel::data::Record,
        _tx: parallel::Sender<Self::Output>,
    ) -> Result<(), errors::ProcessingError> {
        if let Some(vat_number) = &record.vat_number {
            let wiki_ids = self
                .sources
                .eu_ecolabel
                .vat_to_wiki(&models::VatId::try_from(vat_number)?)
                .map(|matching| vec![matching.wiki_id.to_id()]);

            let producer = schema::ReviewProducer {
                id: vat_number.to_string(),
                ids: schema::ProducerIds {
                    vat: Some(vec![vat_number.to_string()]),
                    wiki: wiki_ids,
                    domains: None,
                },
                names: vec![record.product_or_service_name.clone()],
                description: None,
                images: Vec::default(),
                websites: Vec::default(),
                origins: Some(schema::ProducerOrigins { regions: Self::extract_region(&record) }),
                reports: None,
                review: Some(schema::Review::Certification(schema::Certification {
                    is_certified: Some(true),
                })),
            };

            self.collector.insert_producer(producer);

            let ids = match record.code {
                Some(eu_ecolabel::data::Code::Ean13(code)) => Some((
                    code.to_string(),
                    schema::ProductIds {
                        ean: Some(vec![code.to_string()]),
                        gtin: None,
                        wiki: None,
                    },
                )),
                Some(eu_ecolabel::data::Code::Gtin14(code)) => Some((
                    code.to_string(),
                    schema::ProductIds {
                        ean: None,
                        gtin: Some(vec![code.to_string()]),
                        wiki: None,
                    },
                )),
                Some(eu_ecolabel::data::Code::Internal(_) | eu_ecolabel::data::Code::Other(_))
                | None => None,
            };

            if let Some((id, ids)) = ids {
                let product = schema::ReviewProduct {
                    id,
                    ids,
                    names: vec![record.product_or_service_name.clone()],
                    summary: None,
                    images: Vec::new(),
                    categorisation: None,
                    origins: Some(schema::ProductOrigins {
                        producer_ids: vec![vat_number.to_string()],
                        regions: Self::extract_region(&record),
                    }),
                    availability: None,
                    related: None,
                    reports: None,
                    review: Some(schema::Review::Certification(schema::Certification {
                        is_certified: Some(true),
                    })),
                    shopping: None,
                };

                self.collector.add_product(product);
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

struct BCorpCondenser {
    /// Sources configuration.
    config: config::CondensationConfig,
}

impl BCorpCondenser {
    pub fn new(config: config::CondensationConfig) -> Self {
        Self { config }
    }

    /// The IDs of companies used in links to company profiles on the `BCorp` web page
    /// are not provided in the Bcorp data.
    /// Here we make a guess of how that ID looks like basing on company name.
    #[must_use]
    pub fn guess_link_id_from_company_name(name: &str) -> String {
        [
            "https://www.bcorporation.net/en-us/find-a-b-corp/company/",
            &name.to_lowercase().replace(['.', '.'], "").replace(' ', "-"),
            "/",
        ]
        .join("")
    }

    fn extract_origins(
        record: &bcorp::data::Record,
        advisor: &advisors::BCorpAdvisor,
    ) -> Option<schema::ProducerOrigins> {
        match advisor.get_regions(&record.country) {
            Some(models::Regions::List(list)) => Some(schema::ProducerOrigins {
                regions: Some(schema::RegionList(
                    list.iter().map(|code| code.alpha3().to_owned()).collect(),
                )),
            }),
            Some(models::Regions::Unknown | models::Regions::World) | None => {
                log::warn!(
                    "Missing BCorp country mapping to country code for '{}'",
                    record.country
                );
                None
            }
        }
    }
}

#[async_trait]
impl parallel::RefProducer for BCorpCondenser {
    type Output = SaveMessage;
    type Error = errors::ProcessingError;

    async fn produce(&self, tx: parallel::Sender<Self::Output>) -> Result<(), Self::Error> {
        let mut collector = ReviewerCollector::default();

        let advisor = advisors::BCorpAdvisor::load(
            &self.config.origin.bcorp_path,
            &self.config.meta.bcorp_regions_path,
        )?;
        let original_data = bcorp::reader::parse(&self.config.origin.bcorp_path)?;

        // The same company may have multiple records.
        // We use only the latest one.
        let mut filtered_data = HashMap::<String, bcorp::data::Record>::new();
        for record in original_data {
            match filtered_data.entry(record.company_id.clone()) {
                Entry::Occupied(mut entry) => {
                    if entry.get().date_certified < record.date_certified {
                        entry.insert(record);
                    }
                }
                Entry::Vacant(entry) => {
                    let _ = entry.insert(record);
                }
            }
        }

        // Process the filtered records.
        for record in filtered_data.values() {
            collector.insert_producer(schema::ReviewProducer {
                id: record.company_id.clone(),
                ids: schema::ProducerIds {
                    vat: None,
                    wiki: None,
                    domains: Some(vec![extract_domain_from_url(&record.website)]),
                },
                names: vec![record.company_name.clone()],
                description: Some(record.description.clone()),
                images: Vec::new(),
                websites: vec![record.website.clone()],
                origins: Self::extract_origins(record, &advisor),
                reports: Some(schema::Reports(vec![schema::Report {
                    title: Some(record.company_name.clone()),
                    url: Some(Self::guess_link_id_from_company_name(&record.company_name)),
                }])),
                review: Some(schema::Review::Certification(schema::Certification {
                    is_certified: Some(record.current_status.is_certified()),
                })),
            });
        }

        let substrate = collector.build_substrate(AboutBCorp::build());
        tx.send(SaveMessage {
            name: AboutBCorp::name().to_owned(),
            variant: AboutBCorp::variant(),
            substrate,
        })
        .await;

        Ok(())
    }
}

struct FtiCondenser {
    /// Sources configuration.
    config: config::CondensationConfig,
}

impl FtiCondenser {
    pub fn new(config: config::CondensationConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl parallel::RefProducer for FtiCondenser {
    type Output = SaveMessage;
    type Error = errors::ProcessingError;

    async fn produce(&self, tx: parallel::Sender<Self::Output>) -> Result<(), Self::Error> {
        let mut collector = ReviewerCollector::default();

        let data = fashion_transparency_index::reader::parse(
            &self.config.support.fashion_transparency_index_path,
        )?;
        for entry in data {
            collector.insert_producer(schema::ReviewProducer {
                id: entry.name.clone(),
                ids: schema::ProducerIds {
                    vat: None,
                    wiki: entry.wikidata_id.map(|id| vec![id.to_id()]),
                    domains: None,
                },
                names: vec![entry.name],
                description: None,
                images: Vec::new(),
                websites: Vec::new(),
                origins: None,
                reports: None,
                review: Some(schema::Review::Certification(schema::Certification {
                    is_certified: Some(true),
                })),
            });
        }

        let substrate = collector.build_substrate(AboutFti::build());
        tx.send(SaveMessage {
            name: AboutFti::name().to_owned(),
            variant: AboutFti::variant(),
            substrate,
        })
        .await;

        Ok(())
    }
}

struct TcoCondenser {
    /// Sources configuration.
    config: config::CondensationConfig,
}

impl TcoCondenser {
    pub fn new(config: config::CondensationConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl parallel::RefProducer for TcoCondenser {
    type Output = SaveMessage;
    type Error = errors::ProcessingError;

    async fn produce(&self, tx: parallel::Sender<Self::Output>) -> Result<(), Self::Error> {
        let mut collector = ReviewerCollector::default();

        let data = tco::reader::parse(&self.config.support.tco_path)?;
        for entry in data {
            collector.insert_producer(schema::ReviewProducer {
                id: entry.company_name.clone(),
                ids: schema::ProducerIds {
                    vat: None,
                    wiki: Some(vec![entry.wikidata_id.to_id()]),
                    domains: None,
                },
                names: vec![entry.company_name],
                description: None,
                images: Vec::new(),
                websites: Vec::new(),
                origins: None,
                reports: None,
                review: Some(schema::Review::Certification(schema::Certification {
                    is_certified: Some(true),
                })),
            });
        }

        let substrate = collector.build_substrate(AboutTco::build());
        tx.send(SaveMessage {
            name: AboutTco::name().to_owned(),
            variant: AboutTco::variant(),
            substrate,
        })
        .await;

        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct Combiner<A>
where
    A: About,
{
    /// Collected data.
    collector: A::Collector,
}

#[async_trait]
impl<A> parallel::Processor for Combiner<A>
where
    A: About + Clone,
{
    type Input = A::Collector;
    type Output = SaveMessage;
    type Error = errors::CondensationError;

    async fn process(
        &mut self,
        input: Self::Input,
        _tx: parallel::Sender<Self::Output>,
    ) -> Result<(), Self::Error> {
        self.collector.merge(input)?;
        Ok(())
    }

    async fn finish(self, tx: parallel::Sender<Self::Output>) -> Result<(), Self::Error> {
        let name = A::name().to_owned();
        let about = A::build();
        let variant = A::variant();
        let substrate = self.collector.build_substrate(about);
        tx.send(SaveMessage { name, variant, substrate }).await;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct SaveMessage {
    name: String,
    variant: schema::SubstrateExtension,
    substrate: schema::Substrate,
}

pub struct SubstrateSaver {
    config: config::CondensationConfig,
}

impl SubstrateSaver {
    #[must_use]
    pub fn new(config: config::CondensationConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl parallel::Consumer for SubstrateSaver {
    type Input = SaveMessage;
    type Error = errors::CondensationError;

    async fn consume(&mut self, mut input: Self::Input) -> Result<(), Self::Error> {
        std::fs::create_dir_all(&self.config.substrate.substrate_path)
            .map_err(|e| Self::Error::Io(e, self.config.substrate.substrate_path.clone()))?;
        let path = self
            .config
            .substrate
            .substrate_path
            .join(&input.name)
            .with_extension(input.variant.as_str());
        log::info!("Saving {:?}", path);
        input.substrate.sort();
        input.substrate.save(&path)?;
        log::info!("Saved");
        Ok(())
    }

    async fn finish(mut self) -> Result<(), errors::CondensationError> {
        log::info!("Condensation finished");
        Ok(())
    }
}

pub struct CondensingRunner;

impl CondensingRunner {
    pub fn run(config: &config::CondensationConfig) -> Result<(), errors::ProcessingError> {
        let sources = Arc::new(CondensationSources::load(&config.clone())?);
        let mut flow = parallel::Flow::new();

        let (save_tx, save_rx) = parallel::bounded::<SaveMessage>();
        let saver = SubstrateSaver::new(config.clone());
        flow = flow.name("saver").spawn_consumer(saver, save_rx)?;

        if config.group != config::CondensationGroup::Immediate {
            let (wiki_process_tx, wiki_process_rx) = parallel::bounded::<String>();
            let (wiki_combine_tx, wiki_combine_rx) = parallel::bounded::<CatalogerCollector>();
            let wiki_producer = runners::WikidataProducer::new(&config.into())?;
            let wiki_worker = CondensingWikidataWorker::new(sources.clone());
            let wiki_worker = runners::WikidataProcessor::new(wiki_worker);
            let wiki_combiner = Combiner::<AboutWiki>::default();
            flow = flow
                .name("wiki")
                .spawn_producer(wiki_producer, wiki_process_tx)?
                .spawn_processors(wiki_worker, wiki_process_rx, wiki_combine_tx)?
                .spawn_processor(wiki_combiner, wiki_combine_rx, save_tx.clone())?;
        }

        if config.group != config::CondensationGroup::Filtered {
            let (off_process_tx, off_process_rx) =
                parallel::bounded::<runners::OpenFoodFactsRunnerMessage>();
            let (off_combine_tx, off_combine_rx) = parallel::bounded::<CatalogerCollector>();
            let off_producer = runners::OpenFoodFactsProducer::new(config.into())?;
            let off_worker = CondensingOpenFoodFactsWorker::new(sources.clone());
            let off_worker = runners::OpenFoodFactsProcessor::new(off_worker);
            let off_combiner = Combiner::<AboutOff>::default();
            flow = flow
                .name("off")
                .spawn_producer(off_producer, off_process_tx)?
                .spawn_processors(off_worker, off_process_rx, off_combine_tx)?
                .spawn_processor(off_combiner, off_combine_rx, save_tx.clone())?;

            let (eu_process_tx, eu_process_rx) =
                parallel::bounded::<runners::EuEcolabelRunnerMessage>();
            let (eu_combine_tx, eu_combine_rx) = parallel::bounded::<ReviewerCollector>();
            let eu_producer = runners::EuEcolabelProducer::new(config.into())?;
            let eu_worker = CondensingEuEcolabelWorker::new(sources.clone());
            let eu_worker = runners::EuEcolabelProcessor::new(eu_worker);
            let eu_combiner = Combiner::<AboutEu>::default();
            flow = flow
                .name("eu")
                .spawn_producer(eu_producer, eu_process_tx)?
                .spawn_processors(eu_worker, eu_process_rx, eu_combine_tx)?
                .spawn_processor(eu_combiner, eu_combine_rx, save_tx.clone())?;

            let bcorp_producer = Box::new(BCorpCondenser::new(config.clone()));
            let fti_producer = Box::new(FtiCondenser::new(config.clone()));
            let tco_producer = Box::new(TcoCondenser::new(config.clone()));
            flow = flow.name("small").spawn_producers(
                vec![bcorp_producer, fti_producer, tco_producer],
                save_tx.clone(),
            )?;
        }

        drop(save_tx);

        flow.join();
        Ok(())
    }
}
