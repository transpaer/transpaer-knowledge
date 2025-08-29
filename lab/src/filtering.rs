// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{io::Write, sync::Arc};

use async_trait::async_trait;

use transpaer_wikidata::data::{Entity, Item};

use crate::{advisors, config, errors, parallel, runners, wikidata::ItemExt};

const WIKIDATA_SUBSTRATE_NAME: &str = "wikidata";

#[derive(Clone)]
pub struct Message {
    entry: String,
    has_wikipedia_page: bool,
}

/// Filters product entries out from the wikidata dump file.
#[derive(Clone)]
pub struct FilteringWorker {
    wikidata: Arc<advisors::WikidataAdvisor>,
    substrate: Arc<advisors::SubstrateAdvisor>,
}

impl FilteringWorker {
    fn new(
        wikidata: Arc<advisors::WikidataAdvisor>,
        substrate: Arc<advisors::SubstrateAdvisor>,
    ) -> Self {
        Self { wikidata, substrate }
    }

    /// Decides if the passed item should be kept or filtered out.
    ///
    /// The item is kept if it:
    /// - is a product or
    /// - is a manufacturer.
    fn should_keep(&self, item: &Item) -> bool {
        // Is a product or organisation according to wikidata?
        if self.wikidata.is_product(item) || self.wikidata.is_organisation(item) {
            return true;
        }

        // Is a product according to any of the substrates?
        if self.substrate.has_product_wiki_id(&item.id.into()) {
            return true;
        }

        // Is an organisation according to any of the substrates?
        if self.substrate.has_producer_wiki_id(&item.id.into()) {
            return true;
        }
        if let Some(websites) = item.get_official_websites() {
            if self.substrate.has_domains(&websites) {
                return true;
            }
        }

        false
    }
}

#[async_trait]
impl runners::WikidataWorker for FilteringWorker {
    type Output = Message;

    async fn process(
        &mut self,
        msg: &str,
        entity: Entity,
        tx: parallel::Sender<Self::Output>,
    ) -> Result<(), errors::ProcessingError> {
        match entity {
            Entity::Item(item) => {
                let mut has_wikipedia_page = false;
                if self.should_keep(&item) {
                    for sl in item.sitelinks.values() {
                        if sl.site == "enwiki" {
                            has_wikipedia_page = true;
                            break;
                        }
                    }
                    tx.send(Message { entry: msg.to_string(), has_wikipedia_page }).await;
                }
            }
            Entity::Property(_property) => {}
        }
        Ok(())
    }

    async fn finish(
        self,
        _tx: parallel::Sender<Self::Output>,
    ) -> Result<(), errors::ProcessingError> {
        Ok(())
    }
}

/// Filters product entries out from the wikidata dump file.
#[derive(Clone, Debug)]
pub struct FilteringStash {
    /// Filtered Wikidata entries.
    entries: Vec<String>,

    /// Number of all entries.
    all_entries: usize,

    /// Number of entries with a corresponding wikipedia page.
    with_wikipedia_page: usize,

    /// Configuration.
    config: config::FilteringConfig,
}

impl FilteringStash {
    #[must_use]
    pub fn new(config: config::FilteringConfig) -> Self {
        Self { entries: Vec::new(), all_entries: 0, with_wikipedia_page: 0, config }
    }

    pub fn add_input(&mut self, input: Message) {
        self.entries.push(input.entry);
        self.all_entries += 1;
        if input.has_wikipedia_page {
            self.with_wikipedia_page += 1;
        }
    }

    #[must_use]
    pub fn is_full(&self) -> bool {
        self.entries.len() >= 100_000
    }

    #[allow(clippy::unused_self)]
    fn save(&self) -> Result<(), std::io::Error> {
        log::info!("Saving {} entries", self.entries.len());
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.config.wikidata_filtered_dump_path)?;
        for line in &self.entries {
            file.write_all(line.as_bytes())?;
            file.write_all(b"\n")?;
        }

        Ok(())
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

#[async_trait]
impl runners::Stash for FilteringStash {
    type Input = Message;

    fn stash(&mut self, input: Self::Input) -> Result<(), errors::ProcessingError> {
        self.add_input(input);

        // Periodically save data to file to avoid running out of memory.
        if self.is_full() {
            self.save().map_err(|e| {
                errors::ProcessingError::Io(e, self.config.wikidata_filtered_dump_path.clone())
            })?;
            self.clear();
        }

        Ok(())
    }

    fn finish(self) -> Result<(), errors::ProcessingError> {
        self.save().map_err(|e| {
            errors::ProcessingError::Io(e, self.config.wikidata_filtered_dump_path.clone())
        })?;
        log::info!(" - {} processed entries", self.all_entries);
        log::info!(" - {} entries have a corresponding wikipedia page", self.with_wikipedia_page);
        Ok(())
    }
}

pub struct FilteringRunner;

impl FilteringRunner {
    pub fn run(config: &config::FilteringConfig) -> Result<(), errors::ProcessingError> {
        let excludes = maplit::hashset! { WIKIDATA_SUBSTRATE_NAME.to_string() };
        let substrate =
            Arc::new(advisors::SubstrateAdvisor::load(&config.substrate_path, &excludes)?);
        let wikidata = Arc::new(advisors::WikidataAdvisor::load(
            &config.cache.wikidata_cache_path,
            &config.meta.wikidata_regions_path,
            &config.meta.wikidata_categories_path,
        )?);

        let worker = FilteringWorker::new(wikidata, substrate);
        let stash = FilteringStash::new(config.clone());

        let flow = parallel::Flow::new();
        runners::WikidataRunner::flow(flow, config, worker, stash)?.join();

        Ok(())
    }
}
