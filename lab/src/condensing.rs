use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use merge::Merge;

use sustainity_collecting::{bcorp, eu_ecolabel, fashion_transparency_index, open_food_facts, tco};
use sustainity_models::{gather as models, ids::WikiId};
use sustainity_schema as schema;
use sustainity_wikidata::data::{Entity, Item};

use crate::{
    advisors, categories, config, errors, parallel, runners, sources,
    sources::Sourceable,
    utils,
    wikidata::{ignored, ItemExt},
};

const LANG_EN: &str = "en";

fn prepare_meta(variant: schema::ProviderVariant) -> schema::Meta {
    schema::Meta {
        version: "0.0.0".to_owned(),
        variant,
        authors: vec!["Sustainity Development Team".to_owned()],
        title: String::new(),
        description: Some("Data prepared by the Sustainity Development Team".to_owned()),
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

pub trait Collector: Clone + Default + Send + merge::Merge {
    type About: Clone + Send;

    fn build_substrate(self, about: Self::About) -> schema::Root;
}

/// Data storage for gathered data from a cataloger.
///
/// Allows merging different instances.
#[derive(Debug, Clone, Default)]
pub struct CatalogerCollector {
    producers: HashMap<String, schema::CatalogProducer>,
    products: Vec<schema::CatalogProduct>,
}

impl merge::Merge for CatalogerCollector {
    fn merge(&mut self, other: Self) {
        utils::merge_hashmaps_with(&mut self.producers, other.producers, merge_catalog_producers);
        merge::vec::append(&mut self.products, other.products);
    }
}

impl Collector for CatalogerCollector {
    type About = schema::AboutCataloger;

    fn build_substrate(self, about: Self::About) -> schema::Root {
        let mut producers: Vec<schema::CatalogProducer> = self.producers.into_values().collect();
        producers.sort_by(|a, b| a.id.cmp(&b.id));

        schema::Root::CatalogerRoot(schema::CatalogerRoot {
            meta: prepare_meta(schema::ProviderVariant::Cataloger),
            cataloger: about,
            producers,
            products: self.products,
        })
    }
}

impl CatalogerCollector {
    #[must_use]
    pub fn has_producer(&self, producer_id: &str) -> bool {
        self.producers.contains_key(producer_id)
    }

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

impl merge::Merge for ReviewerCollector {
    fn merge(&mut self, other: Self) {
        utils::merge_hashmaps_with(&mut self.producers, other.producers, merge_review_producers);
        merge::vec::append(&mut self.products, other.products);
    }
}

impl Collector for ReviewerCollector {
    type About = schema::AboutReviewer;

    fn build_substrate(self, about: Self::About) -> schema::Root {
        let mut producers: Vec<schema::ReviewProducer> = self.producers.into_values().collect();
        producers.sort_by(|a, b| a.id.cmp(&b.id));

        schema::Root::ReviewerRoot(schema::ReviewerRoot {
            meta: prepare_meta(schema::ProviderVariant::Reviewer),
            reviewer: about,
            producers,
            products: self.products,
        })
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

    pub fn push_product(&mut self, product: schema::ReviewProduct) {
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
        schema::SubstrateExtension::Json
    }

    fn build() -> schema::AboutReviewer {
        schema::AboutReviewer {
            id: "bcorp".to_owned(),
            name: "BCorp".to_owned(),
            description: "Data from the BCorp prepared by the Sustainity Team".to_owned(),
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
        schema::SubstrateExtension::Json
    }

    fn build() -> schema::AboutReviewer {
        schema::AboutReviewer {
            id: "eu_ecolabel".to_owned(),
            name: "EU EcoLabel".to_owned(),
            description: "Data from the EU EcoLabel prepared by the Sustainity Team".to_owned(),
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
        schema::SubstrateExtension::Json
    }

    fn build() -> schema::AboutReviewer {
        schema::AboutReviewer {
            id: "fti".to_owned(),
            name: "Fashion Transparency Index".to_owned(),
            description: "Data from the Fashion Transparency Index prepared by the Sustainity Team"
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
                "Data from the Open Food Facts prepared by the Sustainity Team".to_owned(),
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
        schema::SubstrateExtension::Json
    }

    fn build() -> schema::AboutReviewer {
        schema::AboutReviewer {
            id: "tco".to_owned(),
            name: "TCO".to_owned(),
            description: "Data from the TCO prepared by the Sustainity Team".to_owned(),
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
            description: Some("Data from the Wikidata prepared by the Sustainity Team".to_owned()),
            variant: schema::CatalogVariant::Database,
            website: "https://www.wikidata.org/".to_owned(),
        }
    }
}

#[derive(Clone)]
pub struct CondensingWikidataWorker {
    sources: Arc<sources::FullSources>,
    collector: CatalogerCollector,
}

impl CondensingWikidataWorker {
    #[must_use]
    pub fn new(sources: Arc<sources::FullSources>) -> Self {
        Self { collector: CatalogerCollector::default(), sources }
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
    fn extract_wikidata_categories(item: &Item) -> Vec<Vec<String>> {
        let mut result = Vec::new();
        for (name, wiki_categories) in categories::CATEGORIES {
            if Self::has_categories(item, wiki_categories) {
                result.push(vec![(*name).to_string()]);
            }
        }
        result
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
                    let categories = Self::extract_wikidata_categories(&item);
                    if !categories.is_empty() || !Self::has_categories(&item, ignored::ALL) {
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
                                    .map(sustainity_collecting::data::WikiId::to_id)
                                    .collect(),
                            }),
                            availability: None,
                            related: Some(schema::RelatedProducts {
                                preceded_by: Some(
                                    item.get_follows()?
                                        .unwrap_or_default()
                                        .iter()
                                        .map(sustainity_collecting::data::WikiId::to_id)
                                        .collect(),
                                ),
                                followed_by: Some(
                                    item.get_followed_by()?
                                        .unwrap_or_default()
                                        .iter()
                                        .map(sustainity_collecting::data::WikiId::to_id)
                                        .collect(),
                                ),
                            }),
                        };

                        self.collector.add_product(product);
                    }
                }

                // Collect all organisations
                if self.sources.is_organisation(&item) {
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
    sources: Arc<sources::FullSources>,
    collector: CatalogerCollector,
}

impl CondensingOpenFoodFactsWorker {
    #[must_use]
    pub fn new(sources: Arc<sources::FullSources>) -> Self {
        Self { collector: CatalogerCollector::default(), sources }
    }

    /// Extracts categories from a Wikidata item.
    fn extract_open_food_facts_categories(
        record: &open_food_facts::data::Record,
    ) -> Vec<Vec<String>> {
        let mut result = Vec::<Vec<String>>::new();
        for tag in record.food_groups_tags.split(',') {
            if tag.len() > 3 && tag.starts_with("en:") {
                result.push(vec![tag[3..].to_string()]);
            }
        }
        result
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

    fn get_producer_id(record: &open_food_facts::data::Record) -> String {
        utils::disambiguate_name(&record.brand_owner)
    }

    fn guess_producer_wiki_id(&self, record: &open_food_facts::data::Record) -> Option<WikiId> {
        let name = Self::get_producer_id(record);
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
            let categories = Self::extract_open_food_facts_categories(&record);
            let producer_id = Self::get_producer_id(&record);

            let product = schema::CatalogProduct {
                id: gtin.to_string(),
                ids: schema::ProductIds {
                    ean: None,
                    gtin: Some(vec![gtin.to_string()]),
                    wiki: None,
                },
                names: vec![record.product_name.clone()],
                description: None,
                images: vec![record.image_small_url.clone()],
                categorisation: Some(schema::ProductCategorisation {
                    categories: categories.into_iter().map(schema::ProductCategory).collect(),
                }),
                origins: Some(schema::ProductOrigins { producer_ids: vec![producer_id.clone()] }),
                availability: Some(schema::ProductAvailability {
                    regions: Self::extract_open_food_facts_sell_regions(&record, &self.sources.off),
                }),
                related: None,
            };

            self.collector.add_product(product);

            if !self.collector.has_producer(&producer_id) {
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
    sources: Arc<sources::FullSources>,
    collector: ReviewerCollector,
}

impl CondensingEuEcolabelWorker {
    #[must_use]
    pub fn new(sources: Arc<sources::FullSources>) -> Self {
        Self { collector: ReviewerCollector::default(), sources }
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
        if let Some(vat_number) = record.vat_number {
            let wiki_ids = self
                .sources
                .eu_ecolabel
                .vat_to_wiki(&models::VatId::try_from(&vat_number)?)
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
                report: None,
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
                    names: vec![record.product_or_service_name],
                    summary: None,
                    images: Vec::new(),
                    categorisation: None,
                    origins: Some(schema::ProductOrigins {
                        producer_ids: vec![vat_number.to_string()],
                    }),
                    availability: None,
                    related: None,
                    report: None,
                    review: Some(schema::Review::Certification(schema::Certification {
                        is_certified: Some(true),
                    })),
                };

                self.collector.push_product(product);
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
    config: config::SourcesConfig,
}

impl BCorpCondenser {
    pub fn new(config: config::SourcesConfig) -> Self {
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
}

#[async_trait]
impl parallel::RefProducer for BCorpCondenser {
    type Output = SaveMessage;
    type Error = errors::ProcessingError;

    async fn produce(&self, tx: parallel::Sender<Self::Output>) -> Result<(), Self::Error> {
        let mut collector = ReviewerCollector::default();

        let data = bcorp::reader::parse(&self.config.bcorp_path)?;
        for record in data {
            collector.insert_producer(schema::ReviewProducer {
                id: record.company_id,
                ids: schema::ProducerIds {
                    vat: None,
                    wiki: None,
                    domains: Some(vec![utils::extract_domain_from_url(&record.website)]),
                },
                names: vec![record.company_name.clone()],
                description: None,
                images: Vec::new(),
                websites: vec![record.website],
                report: Some(schema::Report {
                    url: Some(Self::guess_link_id_from_company_name(&record.company_name)),
                }),
                review: Some(schema::Review::Certification(schema::Certification {
                    is_certified: Some(true),
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
    config: config::SourcesConfig,
}

impl FtiCondenser {
    pub fn new(config: config::SourcesConfig) -> Self {
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
            &self.config.fashion_transparency_index_path,
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
                report: None,
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
    config: config::SourcesConfig,
}

impl TcoCondenser {
    pub fn new(config: config::SourcesConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl parallel::RefProducer for TcoCondenser {
    type Output = SaveMessage;
    type Error = errors::ProcessingError;

    async fn produce(&self, tx: parallel::Sender<Self::Output>) -> Result<(), Self::Error> {
        let mut collector = ReviewerCollector::default();

        let data = tco::reader::parse(&self.config.tco_path)?;
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
                report: None,
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
    type Error = errors::ProcessingError;

    async fn process(
        &mut self,
        input: Self::Input,
        _tx: parallel::Sender<Self::Output>,
    ) -> Result<(), Self::Error> {
        self.collector.merge(input);
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
    substrate: schema::Root,
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
    type Error = errors::ProcessingError;

    async fn consume(&mut self, mut input: Self::Input) -> Result<(), Self::Error> {
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

    async fn finish(mut self) -> Result<(), errors::ProcessingError> {
        log::info!("Condensation finished");
        Ok(())
    }
}

pub struct CondensingRunner;

impl CondensingRunner {
    pub fn run(config: &config::CondensationConfig) -> Result<(), errors::ProcessingError> {
        let (wiki_process_tx, wiki_process_rx) = parallel::bounded::<String>();
        let (wiki_combine_tx, wiki_combine_rx) = parallel::bounded::<CatalogerCollector>();
        let (off_process_tx, off_process_rx) =
            parallel::bounded::<runners::OpenFoodFactsRunnerMessage>();
        let (off_combine_tx, off_combine_rx) = parallel::bounded::<CatalogerCollector>();
        let (eu_process_tx, eu_process_rx) =
            parallel::bounded::<runners::EuEcolabelRunnerMessage>();
        let (eu_combine_tx, eu_combine_rx) = parallel::bounded::<ReviewerCollector>();
        let (save_tx, save_rx) = parallel::bounded::<SaveMessage>();

        let sources = Arc::new(sources::FullSources::load(&config.into())?);

        let wiki_producer = runners::WikidataProducer::new(&config.into())?;
        let wiki_worker = CondensingWikidataWorker::new(sources.clone());
        let wiki_worker = runners::WikidataProcessor::new(wiki_worker);
        let wiki_combiner = Combiner::<AboutWiki>::default();

        let off_producer = runners::OpenFoodFactsProducer::new(config.into())?;
        let off_worker = CondensingOpenFoodFactsWorker::new(sources.clone());
        let off_worker = runners::OpenFoodFactsProcessor::new(off_worker);
        let off_combiner = Combiner::<AboutOff>::default();

        let eu_producer = runners::EuEcolabelProducer::new(config.into())?;
        let eu_worker = CondensingEuEcolabelWorker::new(sources.clone());
        let eu_worker = runners::EuEcolabelProcessor::new(eu_worker);
        let eu_combiner = Combiner::<AboutEu>::default();

        let bcorp_producer = Box::new(BCorpCondenser::new(config.sources.clone()));
        let fti_producer = Box::new(FtiCondenser::new(config.sources.clone()));
        let tco_producer = Box::new(TcoCondenser::new(config.sources.clone()));

        let saver = SubstrateSaver::new(config.clone());

        parallel::Flow::new()
            .name("saver")
            .spawn_consumer(saver, save_rx)?
            .name("wiki")
            .spawn_producer(wiki_producer, wiki_process_tx)?
            .spawn_processors(wiki_worker, wiki_process_rx, wiki_combine_tx)?
            .spawn_processor(wiki_combiner, wiki_combine_rx, save_tx.clone())?
            .name("off")
            .spawn_producer(off_producer, off_process_tx)?
            .spawn_processors(off_worker, off_process_rx, off_combine_tx)?
            .spawn_processor(off_combiner, off_combine_rx, save_tx.clone())?
            .name("eu")
            .spawn_producer(eu_producer, eu_process_tx)?
            .spawn_processors(eu_worker, eu_process_rx, eu_combine_tx)?
            .spawn_processor(eu_combiner, eu_combine_rx, save_tx.clone())?
            .name("small")
            .spawn_producers(vec![bcorp_producer, fti_producer, tco_producer], save_tx)?
            .join();

        Ok(())
    }
}
