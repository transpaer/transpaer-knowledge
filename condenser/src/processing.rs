use std::borrow::Borrow;

use async_trait::async_trait;

use consumers_wikidata::data::Entity;

use crate::{errors, future_pool};

/// Trait for roviders of the core data for a `Processor`.
#[async_trait]
pub trait Essential: Sized + Sync + Send {
    type Config;

    /// Loads the data.
    fn load(config: &Self::Config) -> Result<Self, errors::ProcessingError>;

    /// Runs the processing by sending each entry in tha data to the passed channel.
    async fn run(
        &mut self,
        tx: async_channel::Sender<String>,
    ) -> Result<usize, errors::ProcessingError>;
}

/// Trait for structures holding all the supplementary source data required by a `Processor`.
pub trait Sourceable: Sized + Sync + Send {
    type Config;

    /// Loads the data.
    fn load(config: &Self::Config) -> Result<Self, errors::ProcessingError>;
}

/// Trait for data storage enabling gathering data in multiple threads and later marging them.
pub trait Collectable: Default + merge::Merge + Send {}

/// Command processing logic.
#[async_trait]
pub trait Processor: 'static {
    type Config: Clone + Send;
    type Essentials: Essential<Config = Self::Config>;
    type Sources: Sourceable<Config = Self::Config>;
    type Collector: Collectable + 'static;

    /// Runs the processing in multiple threads.
    async fn process(config: Self::Config) -> Result<(), errors::ProcessingError> {
        const CHANNEL_QUEUE_BOUND: usize = 100;
        let mut essentials = Self::Essentials::load(&config)?;
        let sources = std::sync::Arc::new(Self::Sources::load(&config)?);

        let threads = num::clamp(Self::get_num_threads(), 1, num_cpus::get());
        log::info!("Using {threads} threads");

        let mut pool = future_pool::FuturePool::<Self::Collector>::new();
        let (tx, rx) = async_channel::bounded(CHANNEL_QUEUE_BOUND);
        for _ in 0..threads {
            let rx = rx.clone();
            let sources = sources.clone();
            let config = config.clone();
            pool.spawn(Self::handle_messages(rx, sources, config));
        }

        let entries = essentials.run(tx.clone()).await?;
        tx.close();

        log::info!("Processed {entries} entries");

        let collector = pool.join().await?;

        Self::finalize(&collector, &config)?;

        Ok(())
    }

    /// Handles a message from `Essential` implementaion.
    async fn handle_messages(
        rx: async_channel::Receiver<String>,
        sources: std::sync::Arc<Self::Sources>,
        config: Self::Config,
    ) -> Self::Collector {
        let mut collector = Self::Collector::default();
        while let Ok(msg) = rx.recv().await {
            let result: Result<Entity, serde_json::Error> = serde_json::from_str(&msg);
            match result {
                Ok(entity) => {
                    if let Err(err) = Self::handle_entity(
                        &msg,
                        &entity,
                        sources.borrow(),
                        &mut collector,
                        &config,
                    ) {
                        log::error!("Failed to handle an entity: {}", err);
                    }
                }
                Err(err) => {
                    log::error!("Failed to parse an entity: {} \nMessage:\n'{}'\n\n", err, msg);
                }
            }
        }
        collector
    }

    /// Decides in how many theres the processing should run.
    fn get_num_threads() -> usize {
        num_cpus::get() - 1
    }

    /// Handles one Wikidata entity.
    fn handle_entity(
        msg: &str,
        entity: &Entity,
        sources: &Self::Sources,
        collector: &mut Self::Collector,
        config: &Self::Config,
    ) -> Result<(), errors::ProcessingError>;

    /// Finalize processing.
    ///
    /// Usually just saves the result into files.
    fn finalize(
        collector: &Self::Collector,
        config: &Self::Config,
    ) -> Result<(), errors::ProcessingError>;
}
