use std::{collections::HashSet, io::Write};

use async_trait::async_trait;

use consumers_wikidata::data::Entity;

use crate::{
    cache, config, errors, knowledge,
    processing::{Collectable, Essential, Processor, Sourceable},
    wikidata::ItemExt,
};

/// Provides the core data for the processor.
#[derive(Debug)]
pub struct ProductEssentials {
    /// Wikidata dump file loader.
    wiki: consumers_wikidata::dump::Loader,
}

#[async_trait]
impl Essential for ProductEssentials {
    type Config = config::ProductFilterConfig;

    fn load(config: &Self::Config) -> Result<Self, errors::ProcessingError> {
        Ok(Self { wiki: consumers_wikidata::dump::Loader::load(&config.wikidata_dump_path)? })
    }

    async fn run(
        &mut self,
        tx: async_channel::Sender<String>,
    ) -> Result<usize, errors::ProcessingError> {
        Ok(self.wiki.run_with_channel(tx).await?)
    }
}

/// Holds all the supplementary source data.
#[derive(Debug)]
pub struct ProductSources;

impl Sourceable for ProductSources {
    type Config = config::ProductFilterConfig;

    fn load(_config: &Self::Config) -> Result<Self, errors::ProcessingError> {
        Ok(Self)
    }
}

/// Data storage for gathered data.
///
/// Allows merging different instances.
#[derive(Default, Debug)]
pub struct ProductCollector {
    /// IDs of manufacturers.
    manufacturer_ids: HashSet<knowledge::Id>,

    /// Entries in wikidata about manufacturers.
    products: Vec<String>,
}

impl ProductCollector {
    pub fn add_product(&mut self, product: String) {
        self.products.push(product);
    }

    pub fn add_manufacturer_ids(&mut self, ids: &[knowledge::Id]) {
        for id in ids {
            self.manufacturer_ids.insert(id.clone());
        }
    }
}

impl merge::Merge for ProductCollector {
    fn merge(&mut self, other: Self) {
        self.manufacturer_ids.extend(other.manufacturer_ids);
        self.products.extend_from_slice(&other.products);
    }
}

impl Collectable for ProductCollector {}

/// Filters product entries out from the wikidata dump file.
#[derive(Debug)]
pub struct ProductProcessor;

impl Processor for ProductProcessor {
    type Config = config::ProductFilterConfig;
    type Essentials = ProductEssentials;
    type Sources = ProductSources;
    type Collector = ProductCollector;

    /// Handles one Wikidata entity.
    fn handle_entity(
        msg: &str,
        entity: &Entity,
        _sources: &Self::Sources,
        collector: &mut Self::Collector,
    ) {
        match entity {
            Entity::Item(item) => {
                if let Some(manufacturer_ids) = item.get_manufacturer_ids() {
                    collector.add_product(msg.to_string());
                    collector.add_manufacturer_ids(&manufacturer_ids);
                }
            }
            Entity::Property(_property) => (),
        }
    }

    /// Saves the result into files.
    fn save(
        config: &Self::Config,
        collector: &Self::Collector,
    ) -> Result<(), errors::ProcessingError> {
        log::info!("Found {} products", collector.products.len());
        log::info!("Found {} manufacturers", collector.manufacturer_ids.len());

        let cache = cache::Wikidata {
            manufacturer_ids: collector
                .manufacturer_ids
                .iter()
                .map(|id| id.as_string().clone())
                .collect(),
        };

        let contents = serde_json::to_string_pretty(&cache)?;
        std::fs::write(&config.wikidata_cache_path, contents)?;

        let mut file = std::fs::File::create(&config.wikidata_products_path)?;
        for line in &collector.products {
            file.write_all(line.as_bytes())?;
            file.write_all(b"\n")?;
        }

        Ok(())
    }
}
