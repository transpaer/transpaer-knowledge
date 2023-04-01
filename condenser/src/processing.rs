use std::{borrow::Borrow, collections::HashSet};

use consumers_wikidata::data::Entity;

use crate::{
    cache, categories, config, data_collector, future_pool, knowledge, sources, targets, utils,
    wikidata::ItemExt,
};

const LANG_EN: &str = "en";

fn convert_info(info: &consumers_collecting::consumers::data::Info) -> knowledge::Info {
    knowledge::Info {
        id: info.id.clone(),
        title: info.title.clone(),
        article: info.article.clone(),
    }
}

/// Handles on Wikidata entity.
fn handle_entity(
    entity: &Entity,
    sources: &sources::Sources,
    collector: &mut data_collector::DataCollector,
) {
    match entity {
        Entity::Item(item) => {
            if let Some(name) = item.labels.get(LANG_EN).map(|label| &label.value) {
                // Gather all manufacturer IDs and collect products
                if let Some(manufacturer_ids) = item.get_manufacturer_ids() {
                    let category = if item.is_instance_of(categories::SMARTPHONE_MODEL) {
                        Some(knowledge::Category::Smartphone)
                    } else {
                        None
                    };

                    let product = knowledge::Product {
                        id: item.id.clone().into(),
                        name: name.to_string(),
                        description: item
                            .descriptions
                            .get(LANG_EN)
                            .map(|desc| desc.value.clone())
                            .unwrap_or_default(),
                        category,
                        manufacturer_ids: item.get_manufacturer_ids(),
                        follows: item.get_follows(),
                        followed_by: item.get_followed_by(),
                        certifications: knowledge::Certifications::default(),
                    };

                    collector.add_product(product);
                    collector.add_manufacturer_ids(&manufacturer_ids);
                }

                // Collect all manufacturers
                if sources.cache.has_manufacturer_id(&item.id) {
                    let websites = item.get_official_websites();
                    let domains: HashSet<String> = if let Some(websites) = &websites {
                        websites
                            .iter()
                            .map(|website| utils::extract_domain_from_url(website))
                            .collect()
                    } else {
                        HashSet::new()
                    };

                    let is_bcorp = sources.bcorp.has_domains(&domains);
                    let is_tco = sources.tco.has_company(&item.id);
                    let manufacturer = knowledge::Manufacturer {
                        id: item.id.clone().into(),
                        name: name.to_string(),
                        description: item
                            .descriptions
                            .get(LANG_EN)
                            .map(|desc| desc.value.clone())
                            .unwrap_or_default(),
                        websites: websites.unwrap_or_default(),
                        certifications: knowledge::Certifications { bcorp: is_bcorp, tco: is_tco },
                    };
                    collector.add_manufacturer(manufacturer);
                }
            }
        }
        Entity::Property(_property) => (),
    }
}

/// Handles a message from `consumers_wikidata::dump::Loader`
async fn handle_messages(
    rx: async_channel::Receiver<String>,
    sources: std::sync::Arc<sources::Sources>,
) -> data_collector::DataCollector {
    let mut data_collector = data_collector::DataCollector::new();
    while let Ok(msg) = rx.recv().await {
        let result: Result<Entity, serde_json::Error> = serde_json::from_str(&msg);
        match result {
            Ok(entity) => handle_entity(&entity, sources.borrow(), &mut data_collector),
            Err(err) => log::error!("Failed to parse an entity: {} \nMessage:\n'{}'\n\n", err, msg),
        }
    }
    data_collector
}

pub async fn process(config: config::Config) -> Result<(), crate::errors::ProcessingError> {
    const CHANNEL_QUEUE_BOUND: usize = 100;
    let sources = std::sync::Arc::new(sources::Sources::new(&config)?);

    let cpus: usize = std::cmp::max(1, num_cpus::get() - 1);
    log::info!("Using {cpus} CPUs");

    let mut pool = future_pool::FuturePool::<data_collector::DataCollector>::new();
    let (tx, rx) = async_channel::bounded(CHANNEL_QUEUE_BOUND);
    for _ in 0..cpus {
        let rx = rx.clone();
        let sources = sources.clone();
        pool.spawn(handle_messages(rx, sources));
    }

    let entries = consumers_wikidata::dump::Loader::load(&config.wikidata_dump_path)?
        .run_with_channel(tx)
        .await?;

    log::info!("Processed {entries} entries");

    let mut collector = pool.join().await?;
    collector.postprocess();

    cache::Saver::new(config.clone()).save(&collector)?;

    let target_writer = targets::TargetWriter::new(config.clone());
    target_writer.write_collected_data(&collector)?;
    target_writer.write_info_data(
        &sources.consumers.get_info().iter().map(convert_info).collect::<Vec<knowledge::Info>>(),
    )?;

    Ok(())
}
