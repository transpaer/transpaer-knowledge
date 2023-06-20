use std::collections::HashSet;

use sustainity_wikidata::data::Entity;

use crate::{
    cache, config, errors, knowledge,
    processing::{Collectable, Processor, Sourceable},
    runners,
    wikidata::ItemExt,
};

/// Holds all the supplementary source data.
#[derive(Debug)]
pub struct PrefilteringSources;

impl Sourceable for PrefilteringSources {
    type Config = config::PrefilteringConfig;

    fn load(_config: &Self::Config) -> Result<Self, errors::ProcessingError> {
        Ok(Self)
    }
}

/// Data storage for gathered data.
///
/// Allows merging different instances.
#[derive(Default, Debug)]
pub struct PrefilteringCollector {
    /// IDs of manufacturers.
    manufacturer_ids: HashSet<knowledge::WikiStrId>,

    /// IDs of product classes.
    classes: HashSet<knowledge::WikiStrId>,
}

impl PrefilteringCollector {
    pub fn add_manufacturer_ids(&mut self, ids: &[knowledge::WikiStrId]) {
        for id in ids {
            self.manufacturer_ids.insert(id.clone());
        }
    }

    pub fn add_classes(&mut self, classes: &[knowledge::WikiStrId]) {
        self.classes.extend(classes.iter().cloned());
    }
}

impl merge::Merge for PrefilteringCollector {
    fn merge(&mut self, other: Self) {
        self.manufacturer_ids.extend(other.manufacturer_ids);
        self.classes.extend(other.classes);
    }
}

impl Collectable for PrefilteringCollector {}

/// Filters product entries out from the wikidata dump file.
#[derive(Clone, Debug)]
pub struct PrefilteringProcessor;

impl Default for PrefilteringProcessor {
    fn default() -> Self {
        Self
    }
}

impl Processor for PrefilteringProcessor {
    type Config = config::PrefilteringConfig;
    type Sources = PrefilteringSources;
    type Collector = PrefilteringCollector;

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
        collector: Self::Collector,
        _sources: &Self::Sources,
        config: &Self::Config,
    ) -> Result<(), errors::ProcessingError> {
        log::info!("Found {} manufacturers", collector.manufacturer_ids.len());
        log::info!("Found {} products or classes", collector.classes.len());

        let cache = cache::Wikidata {
            manufacturer_ids: collector.manufacturer_ids.iter().cloned().collect(),
            classes: collector.classes.iter().cloned().collect(),
        };

        let contents = serde_json::to_string_pretty(&cache)?;
        std::fs::write(&config.wikidata_cache_path, contents)?;

        Ok(())
    }
}

impl runners::WikidataProcessor for PrefilteringProcessor {
    fn handle_wikidata_entity(
        &self,
        _msg: &str,
        entity: Entity,
        _sources: &Self::Sources,
        collector: &mut Self::Collector,
        _config: &Self::Config,
    ) -> Result<(), errors::ProcessingError> {
        match entity {
            Entity::Item(item) => {
                if let Some(manufacturer_ids) = item.get_manufacturer_ids() {
                    collector.add_manufacturer_ids(&manufacturer_ids);
                }
                if let Some(class_ids) = item.get_superclasses() {
                    collector.add_classes(&class_ids);
                }
                if let Some(class_ids) = item.get_classes() {
                    collector.add_classes(&class_ids);
                }
            }
            Entity::Property(_property) => (),
        }
        Ok(())
    }
}

pub type PrefilteringRunner = runners::WikidataRunner<PrefilteringProcessor>;
