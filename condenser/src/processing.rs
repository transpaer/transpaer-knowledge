use async_trait::async_trait;

use crate::{errors, future_pool};

/// Trait for structures holding all the supplementary source data required by a `Processor`.
pub trait Sourceable: Sized + Sync + Send {
    type Config;

    /// Loads the data.
    fn load(config: &Self::Config) -> Result<Self, errors::ProcessingError>;
}

/// Trait for data storage enabling gathering data in multiple threads and later marging them.
pub trait Collectable: Default + merge::Merge + Send {}

pub trait Processor: Clone + Default + Send {
    type Config: Clone + Send + 'static;
    type Sources: Sourceable;
    type Collector: Collectable + 'static;

    /// Initializes the processor.
    fn initialize(
        &self,
        collector: &mut Self::Collector,
        sources: &Self::Sources,
        config: &Self::Config,
    ) -> Result<(), errors::ProcessingError>;

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

/// Command processing logic.
#[async_trait]
pub trait Runnable<P>: Sized + 'static
where
    P: Processor,
    for<'a> <Self as Runnable<P>>::Config: From<&'a <P as Processor>::Config>,
    for<'a> <P::Sources as Sourceable>::Config: From<&'a <P as Processor>::Config>,
{
    type Config: Clone + Send;
    type Message: Clone + Send;

    /// Runs the processing in multiple threads.
    async fn run(config: P::Config) -> Result<(), errors::ProcessingError> {
        const CHANNEL_QUEUE_BOUND: usize = 10;
        let self_config: Self::Config = (&config).into();

        let runner = Self::load(self_config.clone())?;
        let sources = std::sync::Arc::new(P::Sources::load(&(&config).into())?);

        let threads = Self::get_thread_number();
        log::info!("Using {threads} threads");

        let processor = P::default();
        let mut pool = future_pool::FuturePool::<P::Collector>::new();
        let (tx, rx) = async_channel::bounded(CHANNEL_QUEUE_BOUND);
        for _ in 0..threads {
            let rx = rx.clone();
            let processor = processor.clone();
            let sources = sources.clone();
            let config = config.clone();
            pool.spawn(Self::process(rx, processor, sources, config));
        }

        let entries = runner.gather(tx.clone(), self_config).await?;
        tx.close();

        log::info!("Processed {entries} entries");

        let collector = pool.join().await?;

        processor.finalize(collector, &sources, &config)?;

        Ok(())
    }

    fn get_thread_number() -> usize {
        num_cpus::get()
    }

    /// Loads the data.
    fn load(config: Self::Config) -> Result<Self, errors::ProcessingError>;

    /// Runs the processing by sending each entry in tha data to the passed channel.
    async fn gather(
        mut self,
        tx: async_channel::Sender<Self::Message>,
        config: Self::Config,
    ) -> Result<usize, errors::ProcessingError>;

    async fn process(
        rx: async_channel::Receiver<Self::Message>,
        r: P,
        sources: std::sync::Arc<P::Sources>,
        config: P::Config,
    ) -> P::Collector;
}
