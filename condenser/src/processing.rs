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
pub trait Processor: Clone + Sized + 'static {
    type Config: Clone + Send + AsRef<<<Self as Processor>::Sources as Sourceable>::Config>;
    type Essentials: Essential<Config = Self::Config>;
    type Sources: Sourceable;
    type Collector: Collectable + 'static;

    /// Runs the processing in multiple threads.
    async fn process(self, config: Self::Config) -> Result<(), errors::ProcessingError> {
        const CHANNEL_QUEUE_BOUND: usize = 100;
        let mut essentials = Self::Essentials::load(&config)?;
        let sources = std::sync::Arc::new(Self::Sources::load(config.as_ref())?);

        let threads = std::cmp::max(1, num_cpus::get() - 1);
        log::info!("Using {threads} threads");

        let mut pool = future_pool::FuturePool::<Self::Collector>::new();
        let (tx, rx) = async_channel::bounded(CHANNEL_QUEUE_BOUND);
        for _ in 0..threads {
            let rx = rx.clone();
            let processor = self.clone();
            let sources = sources.clone();
            let config = config.clone();
            pool.spawn(processor.handle_messages(rx, sources, config));
        }

        let entries = essentials.run(tx.clone()).await?;
        tx.close();

        log::info!("Processed {entries} entries");

        let collector = pool.join().await?;

        self.finalize(&collector, &config)?;

        Ok(())
    }

    /// Handles a message from `Essential` implementaion.
    async fn handle_messages(
        self,
        rx: async_channel::Receiver<String>,
        sources: std::sync::Arc<Self::Sources>,
        config: Self::Config,
    ) -> Self::Collector {
        let mut collector = Self::Collector::default();
        while let Ok(msg) = rx.recv().await {
            let result: Result<Entity, serde_json::Error> = serde_json::from_str(&msg);
            match result {
                Ok(entity) => {
                    if let Err(err) =
                        self.handle_entity(&msg, &entity, sources.borrow(), &mut collector, &config)
                    {
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

    /// Handles one Wikidata entity.
    fn handle_entity(
        &self,
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
        &self,
        collector: &Self::Collector,
        config: &Self::Config,
    ) -> Result<(), errors::ProcessingError>;
}
