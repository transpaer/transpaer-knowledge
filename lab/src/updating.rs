// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{cmp::Ordering, collections::HashMap, sync::Arc};

use async_trait::async_trait;

use transpaer_collecting::{bcorp, eu_ecolabel, open_food_facts, transpaer};
use transpaer_models::combine::Combine;

use crate::{advisors, config, errors, parallel, runners, utils, wikidata::ItemExt};

fn compare_items(c1: &(String, usize), c2: &(String, usize)) -> Ordering {
    let cmp = c2.1.cmp(&c1.1);
    match cmp {
        Ordering::Equal => c1.0.cmp(&c2.0),
        Ordering::Less | Ordering::Greater => cmp,
    }
}

#[allow(clippy::needless_pass_by_value)]
fn summarize_countries(
    mut gathered_countries: HashMap<String, usize>,
    country_descriptions: HashMap<String, String>,
    old_regions: &transpaer::reader::RegionMap,
) -> (transpaer::data::Countries, usize) {
    let mut gathered_countries: Vec<(String, usize)> = gathered_countries.drain().collect();
    gathered_countries.sort_by(compare_items);

    let mut new_countries = transpaer::data::Countries::default();
    let mut assigned_refs: usize = 0;
    let mut all_refs: usize = 0;
    for (tag, count) in gathered_countries {
        let description = country_descriptions.get(&tag).cloned();
        let regions = old_regions.get_regions(&tag);
        if regions.is_some() {
            assigned_refs += count;
        }
        all_refs += count;
        new_countries.countries.push(transpaer::data::CountryEntry {
            tag,
            description,
            regions: regions.cloned(),
            count,
        });
    }

    (new_countries, 100 * assigned_refs / all_refs)
}

#[allow(clippy::needless_pass_by_value)]
fn summarize_classes(
    mut gathered_classes: HashMap<String, usize>,
    class_descriptions: HashMap<String, String>,
    old_categories: &transpaer::reader::CategoryMap,
) -> (transpaer::data::Categories, usize, usize) {
    let mut gathered_classes: Vec<(String, usize)> = gathered_classes.drain().collect();
    gathered_classes.sort_by(compare_items);

    let mut new_categories = transpaer::data::Categories::default();
    let mut assigned_refs: usize = 0;
    let mut deleted_refs: usize = 0;
    let mut all_refs: usize = 0;
    for (tag, count) in gathered_classes {
        all_refs += count;

        let entry = old_categories.get(&tag);
        let description = class_descriptions.get(&tag).cloned();

        let categories = if let Some(entry) = &entry {
            if let Some(categories) = &entry.categories
                && !categories.is_empty()
            {
                assigned_refs += count;
            }

            if entry.delete == Some(true) {
                deleted_refs += count;
            }

            transpaer::data::CategoryEntry {
                tag,
                description,
                categories: entry.categories.clone(),
                delete: entry.delete,
                count,
            }
        } else {
            transpaer::data::CategoryEntry {
                tag,
                description,
                categories: None,
                delete: None,
                count,
            }
        };

        new_categories.categories.push(categories);
    }

    (new_categories, 100 * assigned_refs / all_refs, 100 * deleted_refs / all_refs)
}

/// Data storage for gathered data.
///
/// Allows merging different instances.
#[derive(Debug, Clone, Default)]
pub struct OpenFoodFactsCollector {
    /// Gathers all found countries. The count is used for sorting.
    countries: HashMap<String, usize>,

    /// Gathers all found categories. The count is used for sorting.
    categories: HashMap<String, usize>,

    /// Counts all records in the data.
    records: usize,

    /// Counts how many empty sell countries were present.
    empty_sell_count: usize,

    /// Counts how many empty production countries were present.
    empty_production_count: usize,

    /// Counts how many products without categories were present.
    empty_category_count: usize,
}

impl Combine for OpenFoodFactsCollector {
    fn combine(mut o1: Self, o2: Self) -> Self {
        utils::merge_hashmaps_with(&mut o1.countries, o2.countries, |a, b| *a += b);
        utils::merge_hashmaps_with(&mut o1.categories, o2.categories, |a, b| *a += b);

        Self {
            countries: o1.countries,
            categories: o1.categories,
            records: o1.records + o2.records,
            empty_sell_count: o1.empty_sell_count + o2.empty_sell_count,
            empty_production_count: o1.empty_production_count + o2.empty_production_count,
            empty_category_count: o1.empty_category_count + o2.empty_category_count,
        }
    }
}

/// Filters product entries out from the wikidata dump file.
#[derive(Clone, Debug, Default)]
pub struct OpenFoodFactsWorker {
    collector: OpenFoodFactsCollector,
}

#[async_trait]
impl runners::OpenFoodFactsWorker for OpenFoodFactsWorker {
    type Output = OpenFoodFactsCollector;

    async fn process(
        &mut self,
        record: open_food_facts::data::Record,
        _tx: parallel::Sender<Self::Output>,
    ) -> Result<(), errors::ProcessingError> {
        self.collector.records += 1;

        let sell_countries = record.extract_sell_countries();
        if sell_countries.is_empty() {
            self.collector.empty_sell_count += 1;
        } else {
            for tag in sell_countries {
                self.collector.countries.entry(tag).and_modify(|n| *n += 1).or_insert(1);
            }
        }

        let production_countries = record.extract_production_countries();
        if production_countries.is_empty() {
            self.collector.empty_production_count += 1;
        } else {
            for tag in production_countries {
                self.collector.countries.entry(tag).and_modify(|n| *n += 1).or_insert(1);
            }
        }

        let categories = record.extract_categories();
        if categories.is_empty() {
            self.collector.empty_category_count += 1;
        } else {
            for tag in categories {
                self.collector.categories.entry(tag).and_modify(|n| *n += 1).or_insert(1);
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

pub struct OpenFoodFactsStash {
    /// Collected data.
    collector: Option<OpenFoodFactsCollector>,

    /// Map from Open Food Facts country tags to the Transpaer regions.
    regions: transpaer::reader::RegionMap,

    /// Map from Open Food Facts category tags to the Transpaer categories.
    categories: transpaer::reader::CategoryMap,

    /// Configuration.
    config: config::UpdatingConfig,
}

impl OpenFoodFactsStash {
    fn new(config: config::UpdatingConfig) -> Result<Self, errors::ProcessingError> {
        let regions = transpaer::reader::RegionMap::from_countries(
            transpaer::reader::parse_countries(&config.meta.open_food_facts_regions_path)?,
        );
        let categories = transpaer::reader::CategoryMap::from_categories(
            transpaer::reader::parse_categories(&config.meta.open_food_facts_categories_path)?,
        );
        Ok(Self { collector: None, regions, categories, config })
    }
}

#[async_trait]
impl runners::Stash for OpenFoodFactsStash {
    type Input = OpenFoodFactsCollector;

    fn stash(&mut self, input: Self::Input) -> Result<(), errors::ProcessingError> {
        if let Some(collector) = self.collector.take() {
            self.collector = Some(Combine::combine(collector, input));
        } else {
            self.collector = Some(input);
        }
        Ok(())
    }

    fn finish(self) -> Result<(), errors::ProcessingError> {
        let country_descriptions = HashMap::new();
        let category_descriptions = HashMap::new();

        let collector = self.collector.ok_or(errors::ProcessingError::EmptyCollector)?;
        let (countries, country_percentage) =
            summarize_countries(collector.countries, country_descriptions, &self.regions);
        let (categories, category_assigned_percentage, category_unwanted_percentage) =
            summarize_classes(collector.categories, category_descriptions, &self.categories);

        log::info!("Open Food Facts report:");
        log::info!(" - processed {} entries", collector.records);
        log::info!(" - found {} countries", countries.countries.len());
        log::info!("   - {} entries had no sell country", collector.empty_sell_count);
        log::info!("   - {} entries had no production country", collector.empty_production_count);
        log::info!("   - {country_percentage}% of tag use-cases assigned");
        log::info!(" - found {} categories", categories.categories.len());
        log::info!(
            "   - {category_assigned_percentage}% of category use-cases assigned ({category_unwanted_percentage}% unwanted)"
        );

        transpaer::writer::save_countries(
            &countries,
            &self.config.meta.open_food_facts_regions_path,
        )?;
        transpaer::writer::save_categories(
            &categories,
            &self.config.meta.open_food_facts_categories_path,
        )?;

        Ok(())
    }
}

/// Data storage for gathered data.
///
/// Allows merging different instances.
#[derive(Debug, Clone)]
pub struct WikidataCollector {
    /// Gathers all found countries. The count is used for sorting.
    countries: HashMap<String, usize>,

    /// Description of the countries loaded fomr the Wikidata data.
    country_descriptions: HashMap<String, String>,

    /// Gathers all found classes. The count is used for sorting.
    classes: HashMap<String, usize>,

    /// Description of the classes loaded fomr the Wikidata data.
    class_descriptions: HashMap<String, String>,

    /// Counts all entries in the data.
    entries: usize,

    /// Counts all entries in the data that are organisations.
    organisations: usize,

    /// Counts all entries in the data that are products.
    products: usize,
}

impl WikidataCollector {
    pub fn new() -> Self {
        Self {
            countries: HashMap::new(),
            country_descriptions: HashMap::new(),
            classes: HashMap::new(),
            class_descriptions: HashMap::new(),
            entries: 0,
            organisations: 0,
            products: 0,
        }
    }
}

impl Combine for WikidataCollector {
    fn combine(mut o1: Self, o2: Self) -> Self {
        utils::merge_hashmaps_with(&mut o1.countries, o2.countries, |a, b| *a += b);
        utils::merge_hashmaps_with(&mut o1.classes, o2.classes, |a, b| *a += b);
        utils::merge_hashmaps_with(
            &mut o1.country_descriptions,
            o2.country_descriptions,
            |_, _| {},
        );
        utils::merge_hashmaps_with(&mut o1.class_descriptions, o2.class_descriptions, |_, _| {});

        Self {
            countries: o1.countries,
            country_descriptions: o1.country_descriptions,
            classes: o1.classes,
            class_descriptions: o1.class_descriptions,
            entries: o1.entries + o2.entries,
            organisations: o1.organisations + o2.organisations,
            products: o1.products + o2.products,
        }
    }
}

/// Filters product entries out from the wikidata dump file.
#[derive(Clone)]
pub struct WikidataWorker {
    /// Collected data
    collector: WikidataCollector,

    /// Datafrom all substrates
    substrates: Arc<advisors::SubstrateAdvisor>,

    /// Map from Open Food Facts country tags to the Transpaer regions.
    regions: Arc<transpaer::reader::RegionMap>,

    /// Map from Open Food Facts category tags to the Transpaer categories.
    categories: Arc<transpaer::reader::CategoryMap>,
}

impl WikidataWorker {
    fn new(
        substrates: Arc<advisors::SubstrateAdvisor>,
        regions: Arc<transpaer::reader::RegionMap>,
        categories: Arc<transpaer::reader::CategoryMap>,
    ) -> Self {
        Self { collector: WikidataCollector::new(), substrates, regions, categories }
    }

    fn is_country(&self, tag: &str) -> bool {
        self.regions.contains_tag(tag)
    }

    fn is_class(&self, tag: &str) -> bool {
        self.categories.contains_tag(tag)
    }

    fn process_countries(
        &mut self,
        item: &transpaer_wikidata::data::Item,
    ) -> Result<(), errors::ProcessingError> {
        if let Some(countries) = item.get_countries()? {
            for country in countries {
                let tag = country.to_str_id().into_string();
                self.collector.countries.entry(tag).and_modify(|n| *n += 1).or_insert(1);
            }
        }
        Ok(())
    }

    fn process_categories(
        &mut self,
        item: &transpaer_wikidata::data::Item,
    ) -> Result<(), errors::ProcessingError> {
        if let Some(classes) = item.get_classes()? {
            for class in classes {
                let id = class.to_str_id().into_string();
                self.collector.classes.entry(id).and_modify(|n| *n += 1).or_insert(1);
            }
        }
        if let Some(classes) = item.get_superclasses()? {
            for class in classes {
                let id = class.to_str_id().into_string();
                self.collector.classes.entry(id).and_modify(|n| *n += 1).or_insert(1);
            }
        }
        Ok(())
    }

    fn process_country(&mut self, tag: &str, item: &transpaer_wikidata::data::Item) {
        if let Some(label) = item.get_label(transpaer_wikidata::data::Language::En) {
            self.collector.country_descriptions.insert(tag.to_string(), label.to_string());
        }
    }

    fn process_class(&mut self, tag: &str, item: &transpaer_wikidata::data::Item) {
        if let Some(label) = item.get_label(transpaer_wikidata::data::Language::En) {
            self.collector.class_descriptions.insert(tag.to_string(), label.to_string());
        }
    }
}

#[async_trait]
impl runners::WikidataWorker for WikidataWorker {
    type Output = WikidataCollector;

    async fn process(
        &mut self,
        _msg: &str,
        entity: transpaer_wikidata::data::Entity,
        _tx: parallel::Sender<Self::Output>,
    ) -> Result<(), errors::ProcessingError> {
        self.collector.entries += 1;

        match entity {
            transpaer_wikidata::data::Entity::Item(item) => {
                let tag = item.id.to_str_id().into_string();
                let id = item.id.into();
                if self.substrates.has_producer_wiki_id(&id) {
                    self.collector.organisations += 1;
                    self.process_countries(&item)?;
                }
                if self.substrates.has_product_wiki_id(&id) {
                    self.collector.products += 1;
                    self.process_categories(&item)?;
                }
                if self.is_country(&tag) {
                    self.process_country(&tag, &item);
                }
                if self.is_class(&tag) {
                    self.process_class(&tag, &item);
                }
            }
            transpaer_wikidata::data::Entity::Property(_) => {}
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

pub struct WikidataStash {
    /// Collected data.
    collector: Option<WikidataCollector>,

    /// Map from Open Food Facts country tags to the Transpaer regions.
    regions: Arc<transpaer::reader::RegionMap>,

    /// Map from Open Food Facts category tags to the Transpaer categories.
    categories: Arc<transpaer::reader::CategoryMap>,

    /// Configuration.
    config: config::UpdatingConfig,
}

impl WikidataStash {
    fn new(
        config: config::UpdatingConfig,
        regions: Arc<transpaer::reader::RegionMap>,
        categories: Arc<transpaer::reader::CategoryMap>,
    ) -> Self {
        Self { collector: None, regions, categories, config }
    }
}

#[async_trait]
impl runners::Stash for WikidataStash {
    type Input = WikidataCollector;

    fn stash(&mut self, input: Self::Input) -> Result<(), errors::ProcessingError> {
        if let Some(collector) = self.collector.take() {
            self.collector = Some(Combine::combine(collector, input));
        } else {
            self.collector = Some(input);
        }
        Ok(())
    }

    fn finish(self) -> Result<(), errors::ProcessingError> {
        let collector = self.collector.ok_or(errors::ProcessingError::EmptyCollector)?;
        let (countries, country_percentage) =
            summarize_countries(collector.countries, collector.country_descriptions, &self.regions);
        let (categories, class_assigned_percentage, class_unwanted_percentage) =
            summarize_classes(collector.classes, collector.class_descriptions, &self.categories);

        log::info!("Wikidata report:");
        log::info!(" - processed {} entries)", collector.entries);
        log::info!("   - {} were organisations", collector.organisations);
        log::info!("   - {} were products", collector.products);
        log::info!(" - found {} countries", countries.countries.len());
        log::info!("   - {country_percentage}% of country use-cases assigned");
        log::info!(" - found {} classes", categories.categories.len());
        log::info!(
            "   - {class_assigned_percentage}% of class use-cases assigned ({class_unwanted_percentage}% unwanted)"
        );

        transpaer::writer::save_countries(&countries, &self.config.meta.wikidata_regions_path)?;
        transpaer::writer::save_categories(
            &categories,
            &self.config.meta.wikidata_categories_path,
        )?;

        Ok(())
    }
}

#[derive(derive_new::new)]
struct BCorpsWorker {
    /// Configuration.
    config: config::UpdatingConfig,
}

#[async_trait]
impl parallel::Isolate for BCorpsWorker {
    type Error = errors::ProcessingError;

    async fn process(self) -> Result<(), errors::ProcessingError> {
        let regions = transpaer::reader::RegionMap::from_countries(
            transpaer::reader::parse_countries(&self.config.meta.bcorp_regions_path)?,
        );

        let data = bcorp::reader::parse(&self.config.bcorp_original_path)?;
        let mut gathered_countries = HashMap::<String, usize>::new();
        for record in data {
            gathered_countries.entry(record.country).and_modify(|n| *n += 1).or_insert(1);
        }

        let country_descriptions = HashMap::new();
        let (countries, percentage) =
            summarize_countries(gathered_countries, country_descriptions, &regions);

        log::info!("BCorps report:");
        log::info!(" - found {} countries", countries.countries.len());
        log::info!("   - {percentage}% of tag use-cases assigned");

        transpaer::writer::save_countries(&countries, &self.config.meta.bcorp_regions_path)?;
        Ok(())
    }
}

#[derive(derive_new::new)]
struct EuEcolabelWorker {
    /// Configuration.
    config: config::UpdatingConfig,
}

#[async_trait]
impl parallel::Isolate for EuEcolabelWorker {
    type Error = errors::ProcessingError;

    async fn process(self) -> Result<(), errors::ProcessingError> {
        let regions = transpaer::reader::RegionMap::from_countries(
            transpaer::reader::parse_countries(&self.config.meta.eu_ecolabel_regions_path)?,
        );

        let data = eu_ecolabel::reader::parse(&self.config.eu_ecolabel.eu_ecolabel_path)?;
        let mut gathered_countries = HashMap::<String, usize>::new();
        for record in data {
            gathered_countries.entry(record.company_country).and_modify(|n| *n += 1).or_insert(1);
        }

        let country_descriptions = HashMap::new();
        let (countries, percentage) =
            summarize_countries(gathered_countries, country_descriptions, &regions);

        log::info!("EU Ecolabel report:");
        log::info!(" - found {} countries", countries.countries.len());
        log::info!("   - {percentage}% of tag use-cases assigned");

        transpaer::writer::save_countries(&countries, &self.config.meta.eu_ecolabel_regions_path)?;
        Ok(())
    }
}

pub struct UpdateRunner;

impl UpdateRunner {
    #[allow(clippy::similar_names)]
    pub fn flow(
        config: &config::UpdatingConfig,
    ) -> Result<parallel::Flow, errors::ProcessingError> {
        let substrate =
            Arc::new(advisors::SubstrateAdvisor::load_all(&config.substrate.substrate_path)?);
        let wikidata_regions = Arc::new(transpaer::reader::RegionMap::from_countries(
            transpaer::reader::parse_countries(&config.meta.wikidata_regions_path)?,
        ));
        let wikidata_categories = Arc::new(transpaer::reader::CategoryMap::from_categories(
            transpaer::reader::parse_categories(&config.meta.wikidata_categories_path)?,
        ));

        let off_producer = runners::OpenFoodFactsProducer::new(config.into())?;
        let off_worker = OpenFoodFactsWorker::default();
        let off_processor = runners::OpenFoodFactsProcessor::new(off_worker);
        let off_stash = OpenFoodFactsStash::new(config.clone())?;
        let off_consumer = runners::RunnerConsumer::new(off_stash);

        let wiki_producer = runners::WikidataProducer::new(&config.into())?;
        let wiki_worker =
            WikidataWorker::new(substrate, wikidata_regions.clone(), wikidata_categories.clone());
        let wiki_processor = runners::WikidataProcessor::new(wiki_worker);
        let wiki_stash = WikidataStash::new(config.clone(), wikidata_regions, wikidata_categories);
        let wiki_consumer = runners::RunnerConsumer::new(wiki_stash);

        let eu_ecolabel_isolate = EuEcolabelWorker::new(config.clone());
        let bcorps_isolate = BCorpsWorker::new(config.clone());

        let (off_tx1, off_rx1) = parallel::bounded::<runners::OpenFoodFactsRunnerMessage>();
        let (off_tx2, off_rx2) = parallel::bounded::<OpenFoodFactsCollector>();
        let (wiki_tx1, wiki_rx1) = parallel::bounded::<String>();
        let (wiki_tx2, wiki_rx2) = parallel::bounded::<WikidataCollector>();

        let flow = parallel::Flow::new()
            .name("wiki")
            .spawn_producer(wiki_producer, wiki_tx1)?
            .spawn_processors(wiki_processor, wiki_rx1, wiki_tx2.clone())?
            .spawn_consumer(wiki_consumer, wiki_rx2)?
            .name("off")
            .spawn_producer(off_producer, off_tx1)?
            .spawn_processors(off_processor, off_rx1, off_tx2.clone())?
            .spawn_consumer(off_consumer, off_rx2)?
            .name("bcorp")
            .spawn_isolate(bcorps_isolate)?
            .name("eu_ecolabel")
            .spawn_isolate(eu_ecolabel_isolate)?;

        Ok(flow)
    }

    pub fn run(config: &config::UpdatingConfig) -> Result<(), errors::ProcessingError> {
        Self::flow(config)?.join();
        Ok(())
    }
}
