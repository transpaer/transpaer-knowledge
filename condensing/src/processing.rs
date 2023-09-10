use async_trait::async_trait;

use crate::{errors, future_pool};

/// Trait for structures holding all the supplementary source data required by a `Processor`.
pub trait Sourceable: Sized + Sync + Send {
    type Config: Clone + Send;

    /// Loads the data.
    ///
    /// # Errors
    ///
    /// Returns `Err` if fails to read from `path`, fails to parse the contents or the contents are invalid.
    fn load(config: &Self::Config) -> Result<Self, errors::ProcessingError>;
}

/// Trait for data storage enabling gathering data in multiple threads and later marging them.
pub trait Collectable: Default + Clone + merge::Merge + Sync + Send {}

/// Trait for all processors.
#[async_trait]
pub trait Processor: Clone + Default + Sync + Send + 'static {
    type Config: Clone + Sync + Send + 'static;
    type Sources: Sourceable + 'static;
    type Collector: Collectable + 'static;

    /// Initializes the processing.
    ///
    /// Usually just fills the collector with initial data.
    fn initialize(
        &self,
        _collector: &mut Self::Collector,
        _sources: &Self::Sources,
        _config: &Self::Config,
    ) -> Result<(), errors::ProcessingError> {
        Ok(())
    }

    /// Finalize processing.
    ///
    /// Usually just saves the result into files.
    fn finalize(
        &self,
        collector: Self::Collector,
        sources: &Self::Sources,
        config: &Self::Config,
    ) -> Result<(), errors::ProcessingError>;
}

/// Trait for structs responsible for loading data from files.
#[async_trait]
pub trait Gatherer: Send + 'static {
    /// Reads the data from files and passes them to `Forwarders`.
    async fn gather(mut self) -> Result<usize, errors::ProcessingError>;
}

/// Trait for structs responsible to handling data loaded by `Gatherers` in many thread and
/// passing them to processors.
#[async_trait]
pub trait Forwarder<P>: Clone + Sync + Send + 'static
where
    P: Processor,
{
    /// Spawns a task responsible for handling messages from the assiociated `Gatherer`.
    async fn spawn(
        self,
        pool: &mut future_pool::FuturePool<P::Collector>,
        processor: P,
        collector: P::Collector,
        sources: std::sync::Arc<P::Sources>,
        config: P::Config,
    );
}

/// Command-processing logic.
#[async_trait]
pub trait Runnable<P>: Sized
where
    P: Processor,
    for<'a> Self::Config: From<&'a <P as Processor>::Config>,
    for<'a> <P::Sources as Sourceable>::Config: From<&'a <P as Processor>::Config>,
{
    type Config: Clone + Send;
    type Gatherer: Gatherer;
    type Forwarder: Forwarder<P>;

    /// Runs the processing in multiple threads.
    async fn run(config: P::Config) -> Result<(), errors::ProcessingError> {
        let self_config = Self::Config::from(&config);
        let sources_config = <<P as Processor>::Sources as Sourceable>::Config::from(&config);

        let (gatherer, forwarder) = Self::create(self_config.clone())?;
        let sources = std::sync::Arc::new(P::Sources::load(&sources_config)?);

        let threads = Self::get_thread_number();
        log::info!("Using {threads} threads");

        let mut collector = P::Collector::default();
        let processor = P::default();
        processor.initialize(&mut collector, &sources, &config)?;

        let mut pool = future_pool::FuturePool::<P::Collector>::default();
        for _ in 0..threads {
            forwarder
                .clone()
                .spawn(
                    &mut pool,
                    processor.clone(),
                    collector.clone(),
                    sources.clone(),
                    config.clone(),
                )
                .await;
        }

        let entries = gatherer.gather().await?;

        log::info!("Processed {entries} entries");

        let collector = pool.join().await?;

        log::info!("Finalizing...");

        processor.finalize(collector, &sources, &config)?;

        Ok(())
    }

    #[must_use]
    fn get_thread_number() -> usize {
        num_cpus::get()
    }

    /// Creates a pair of gatherer and forwarder.
    ///
    /// Gatherer is responsible for loading data from files, and forwarder is responsible for
    /// handling those data in many thread and passing them to processors.
    fn create(
        config: Self::Config,
    ) -> Result<(Self::Gatherer, Self::Forwarder), errors::ProcessingError>;
}
