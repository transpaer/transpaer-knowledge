use std::{cmp::Ordering, collections::HashMap};

use async_trait::async_trait;
use merge::Merge;

use sustainity_collecting::{errors::MapSerde, open_food_facts};

use crate::{config, convert, errors, parallel, runners, sources, sources::Sourceable, utils};

fn compare_countries(c1: &(String, usize), c2: &(String, usize)) -> Ordering {
    let cmp = c2.1.cmp(&c1.1);
    match cmp {
        Ordering::Equal => c1.0.cmp(&c2.0),
        Ordering::Less | Ordering::Greater => cmp,
    }
}

/// Data storage for gathered data.
///
/// Allows merging different instances.
#[derive(Debug, Clone, Default)]
pub struct UpdateCollector {
    /// Gathers all found countries. The count is used for sorting.
    countries: HashMap<String, usize>,

    /// Counts how many empty countries were present.
    empty_count: usize,
}

impl UpdateCollector {}

impl merge::Merge for UpdateCollector {
    fn merge(&mut self, other: Self) {
        utils::merge_hashmaps_with(&mut self.countries, other.countries, |a, b| *a += b);
        self.empty_count += other.empty_count;
    }
}

/// Filters product entries out from the wikidata dump file.
#[derive(Clone, Debug, Default)]
pub struct UpdateWorker {
    collector: UpdateCollector,
}

#[async_trait]
impl runners::OpenFoodFactsWorker for UpdateWorker {
    type Output = UpdateCollector;

    async fn process(
        &mut self,
        record: open_food_facts::data::Record,
        _tx: parallel::Sender<Self::Output>,
    ) -> Result<(), errors::ProcessingError> {
        let sell_countries = record.extract_sell_countries();
        if sell_countries.is_empty() {
            self.collector.empty_count += 1;
        } else {
            for tag in sell_countries {
                self.collector.countries.entry(tag).and_modify(|n| *n += 1).or_insert(1);
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

pub struct UpdateStash {
    /// Collected data.
    collector: UpdateCollector,

    /// Additional data sources.
    sources: sources::FullSources,

    /// Configuration.
    config: config::UpdatingConfig,
}

impl UpdateStash {
    fn new(sources: sources::FullSources, config: config::UpdatingConfig) -> Self {
        Self { collector: UpdateCollector::default(), sources, config }
    }
}

#[async_trait]
impl runners::Stash for UpdateStash {
    type Input = UpdateCollector;

    fn stash(&mut self, input: Self::Input) -> Result<(), errors::ProcessingError> {
        self.collector.merge(input);
        Ok(())
    }

    fn finish(mut self) -> Result<(), errors::ProcessingError> {
        let mut counted_countries: Vec<(String, usize)> =
            self.collector.countries.drain().collect();
        counted_countries.sort_by(compare_countries);

        let mut countries = Vec::<open_food_facts::data::CountryEntry>::new();
        let mut assigned: usize = 0;
        let mut all: usize = 0;
        for (country_tag, count) in counted_countries {
            let regions = self.sources.off.get_countries(&country_tag).map(convert::to_off_regions);
            if regions.is_some() {
                assigned += count;
            }
            all += count;
            countries.push(open_food_facts::data::CountryEntry { country_tag, regions, count });
        }

        println!(" - found {} countries", countries.len(),);
        println!(" - {} entries had no country", self.collector.empty_count,);
        println!(" - {}% of tag use-cases assigned", 100 * assigned / all);

        let contents = serde_yaml::to_string(&countries).map_serde()?;
        std::fs::write(&self.config.sources.open_food_facts_countries_path, contents)?;

        Ok(())
    }
}

pub struct UpdateRunner;

impl UpdateRunner {
    pub fn run(config: &config::UpdatingConfig) -> Result<(), errors::ProcessingError> {
        let sources = sources::FullSources::load(&config.into())?;

        let worker = UpdateWorker::default();
        let stash = UpdateStash::new(sources, config.clone());

        let flow = parallel::Flow::new();
        runners::OpenFoodFactsRunner::flow(flow, config, worker, stash)?.join();

        Ok(())
    }
}
