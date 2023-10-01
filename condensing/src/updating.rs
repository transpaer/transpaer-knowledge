use std::collections::HashMap;

use sustainity_collecting::{errors::MapSerde, open_food_facts};

use crate::{
    config, errors,
    processing::{Collectable, Processor},
    runners, sources, utils,
};

/// Data storage for gathered data.
///
/// Allows merging different instances.
#[derive(Default, Debug, Clone)]
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

impl Collectable for UpdateCollector {}

#[derive(Clone, Debug, Default)]
pub struct UpdateProcessor;

impl Processor for UpdateProcessor {
    type Config = config::UpdatingConfig;
    type Sources = sources::FullSources;
    type Collector = UpdateCollector;

    fn finalize(
        &self,
        mut collector: Self::Collector,
        sources: &Self::Sources,
        config: &Self::Config,
    ) -> Result<(), errors::ProcessingError> {
        let mut counted_countries: Vec<(String, usize)> = collector.countries.drain().collect();
        counted_countries.sort_by(|a, b| b.1.cmp(&a.1));

        let mut countries = Vec::<open_food_facts::data::CountryEntry>::new();
        let mut assigned: usize = 0;
        let mut all: usize = 0;
        for (country_tag, count) in counted_countries {
            let regions =
                sources.off.get_countries(&country_tag).map(open_food_facts::data::Regions::from);
            if regions.is_some() {
                assigned += count;
            }
            all += count;
            countries.push(open_food_facts::data::CountryEntry { country_tag, regions, count });
        }

        println!(" - found {} countries", countries.len(),);
        println!(" - {} entries had no country", collector.empty_count,);
        println!(" - {}% of tag use-cases assigned", 100 * assigned / all);

        let contents = serde_yaml::to_string(&countries).map_serde()?;
        std::fs::write(&config.sources.open_food_facts_countries_path, contents)?;

        Ok(())
    }
}

impl runners::OpenFoodFactsProcessor for UpdateProcessor {
    fn process_open_food_facts_record(
        &self,
        record: open_food_facts::data::Record,
        _sources: &Self::Sources,
        collector: &mut Self::Collector,
        _config: &Self::Config,
    ) -> Result<(), errors::ProcessingError> {
        let sell_countries = record.extract_sell_countries();
        if sell_countries.is_empty() {
            collector.empty_count += 1;
        } else {
            for tag in sell_countries {
                collector.countries.entry(tag).and_modify(|n| *n += 1).or_insert(1);
            }
        }
        Ok(())
    }
}

pub type UpdateRunner = runners::OpenFoodFactsRunner<UpdateProcessor>;
