use std::{io::Write, sync::Arc};

use async_trait::async_trait;

use sustainity_wikidata::data::{Entity, Item};

use crate::{config, errors, parallel, runners, sources, sources::Sourceable};

/// Filters product entries out from the wikidata dump file.
#[derive(Clone)]
pub struct FilteringWorker {
    sources: Arc<sources::FullSources>,
}

impl FilteringWorker {
    fn new(sources: Arc<sources::FullSources>) -> Self {
        Self { sources }
    }

    /// Decides if the passed item should be kept or filtered out.
    ///
    /// The item is kept if it:
    /// - is a product or
    /// - is a manufacturer.
    fn should_keep(&self, item: &Item) -> bool {
        self.sources.is_product(item) || self.sources.is_organisation(item)
    }
}

#[async_trait]
impl runners::WikidataWorker for FilteringWorker {
    type Output = String;

    async fn process(
        &mut self,
        msg: &str,
        entity: Entity,
        tx: parallel::Sender<Self::Output>,
    ) -> Result<(), errors::ProcessingError> {
        match entity {
            Entity::Item(item) => {
                if self.should_keep(&item) {
                    tx.send(msg.to_string()).await;
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
    x: usize,

    /// Configuration.
    config: config::FilteringConfig,
}

impl FilteringStash {
    #[must_use]
    pub fn new(config: config::FilteringConfig) -> Self {
        Self { entries: Vec::new(), x: 0, config }
    }

    pub fn add_entry(&mut self, entry: String) {
        self.x += 1;
        self.entries.push(entry);
    }

    #[must_use]
    pub fn is_full(&self) -> bool {
        self.entries.len() >= 10000
    }

    #[allow(clippy::unused_self)]
    fn save(&self) -> Result<(), errors::ProcessingError> {
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
    type Input = String;

    fn stash(&mut self, entry: Self::Input) -> Result<(), errors::ProcessingError> {
        self.add_entry(entry);

        // Periodically save data to file to avoid running out of memory.
        if self.is_full() {
            self.save()?;
            self.clear();
        }

        Ok(())
    }

    fn finish(self) -> Result<(), errors::ProcessingError> {
        self.save()?;
        Ok(())
    }
}

pub struct FilteringRunner;

impl FilteringRunner {
    pub fn run(config: &config::FilteringConfig) -> Result<(), errors::ProcessingError> {
        let sources = Arc::new(sources::FullSources::load(&config.into())?);

        let worker = FilteringWorker::new(sources);
        let stash = FilteringStash::new(config.clone());

        let flow = parallel::Flow::new();
        runners::WikidataRunner::flow(flow, config, worker, stash)?.join();

        Ok(())
    }
}
