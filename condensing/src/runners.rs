use std::borrow::Borrow;

use async_trait::async_trait;

use sustainity_collecting::{eu_ecolabel, open_food_facts};
use sustainity_wikidata::data::Entity;

use crate::{
    config, errors, future_pool,
    processing::{Forwarder, Gatherer, Processor, Runnable, Sourceable},
};

/// Processor trait for the `WikidataRunner`.
pub trait WikidataProcessor: Processor {
    /// Handles one Wikidata entity.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the processing of the entry fails in any way.
    fn process_wikidata_entity(
        &self,
        msg: &str,
        entity: Entity,
        sources: &Self::Sources,
        collector: &mut Self::Collector,
        config: &Self::Config,
    ) -> Result<(), errors::ProcessingError>;
}

/// Async reader for Wikidata data.
#[must_use]
#[derive(Debug)]
pub struct WikidataGatherer {
    wiki: sustainity_wikidata::dump::Loader,
    tx: async_channel::Sender<String>,
}

impl WikidataGatherer {
    /// Constructs a new `WikidataGatherer`
    ///
    /// # Errors
    ///
    /// Returns `Err` if loading of Wikidata data fails.
    pub fn new(
        tx: async_channel::Sender<String>,
        config: &config::WikidataGathererConfig,
    ) -> Result<Self, errors::ProcessingError> {
        Ok(Self { wiki: sustainity_wikidata::dump::Loader::load(&config.wikidata_path)?, tx })
    }
}

#[async_trait]
impl Gatherer for WikidataGatherer {
    async fn gather(mut self) -> Result<usize, errors::ProcessingError> {
        let num = self
            .wiki
            .run(move |s: String| {
                let tx = self.tx.clone();
                async move {
                    if let Err(err) = tx.send(s).await {
                        log::error!("Failed to send message over channel: {err}");
                    }
                }
            })
            .await?;
        Ok(num)
    }
}

/// Feeds the processor with Wikidata data.
#[must_use]
#[derive(Debug, Clone)]
pub struct WikidataForwarder<P>
where
    P: WikidataProcessor,
{
    rx: async_channel::Receiver<String>,
    phantom: std::marker::PhantomData<P>,
}

impl<P> WikidataForwarder<P>
where
    P: WikidataProcessor,
{
    pub fn new(rx: async_channel::Receiver<String>) -> Self {
        Self { rx, phantom: std::marker::PhantomData }
    }

    /// Handles a message from the `gather`.
    async fn forward(
        self,
        processor: P,
        mut collector: P::Collector,
        sources: std::sync::Arc<P::Sources>,
        config: P::Config,
    ) -> P::Collector {
        while let Ok(msg) = self.rx.recv().await {
            let result: Result<Entity, serde_json::Error> = serde_json::from_str(&msg);
            match result {
                Ok(entity) => {
                    if let Err(err) = processor.process_wikidata_entity(
                        &msg,
                        entity,
                        sources.borrow(),
                        &mut collector,
                        &config,
                    ) {
                        log::error!("Failed to handle a Wikidata entity: {}", err);
                    }
                }
                Err(err) => {
                    log::error!(
                        "Failed to parse a Wikidata entity: {} \nMessage:\n'{}'\n\n",
                        err,
                        msg
                    );
                }
            }
        }
        collector
    }
}

#[async_trait]
impl<P> Forwarder<P> for WikidataForwarder<P>
where
    P: WikidataProcessor,
{
    async fn spawn(
        self,
        pool: &mut future_pool::FuturePool<P::Collector>,
        processor: P,
        collector: P::Collector,
        sources: std::sync::Arc<P::Sources>,
        config: P::Config,
    ) {
        pool.spawn(self.forward(processor, collector, sources, config));
    }
}

pub struct WikidataRunner<P>
where
    P: WikidataProcessor,
{
    phantom: std::marker::PhantomData<P>,
}

#[async_trait]
impl<P> Runnable<P> for WikidataRunner<P>
where
    P: WikidataProcessor + 'static,
    for<'a> config::WikidataGathererConfig: From<&'a <P as Processor>::Config>,
    for<'a> <P::Sources as Sourceable>::Config: From<&'a <P as Processor>::Config>,
{
    type Config = config::WikidataGathererConfig;
    type Gatherer = WikidataGatherer;
    type Forwarder = WikidataForwarder<P>;

    fn create(
        config: Self::Config,
    ) -> Result<(Self::Gatherer, Self::Forwarder), errors::ProcessingError> {
        const CHANNEL_QUEUE_BOUND: usize = 10;
        let (tx, rx) = async_channel::bounded(CHANNEL_QUEUE_BOUND);
        let gatherer = WikidataGatherer::new(tx, &config)?;
        let forwarder = WikidataForwarder::new(rx);
        Ok((gatherer, forwarder))
    }
}

/// Processor trait for the `OpenFoodFactsRunner`.
pub trait OpenFoodFactsProcessor: Processor {
    /// Handles one Open Food Facts record.
    ///
    /// # Errors
    ///
    /// Returns `Err` if processing of the record fails in any way.
    fn process_open_food_facts_record(
        &self,
        record: open_food_facts::data::Record,
        sources: &Self::Sources,
        collector: &mut Self::Collector,
        config: &Self::Config,
    ) -> Result<(), errors::ProcessingError>;
}

/// Message send throught `OpenFoodFactsRunner` channel.
#[derive(Clone, Debug)]
pub struct OpenFoodFactsRunnerMessage {
    record: csv::StringRecord,
    headers: csv::StringRecord,
}

/// Async reader for Open Food Facts data.
#[must_use]
#[derive(Debug)]
pub struct OpenFoodFactsGatherer {
    tx: async_channel::Sender<OpenFoodFactsRunnerMessage>,
    config: config::OpenFoodFactsGathererConfig,
}

impl OpenFoodFactsGatherer {
    pub fn new(
        tx: async_channel::Sender<OpenFoodFactsRunnerMessage>,
        config: config::OpenFoodFactsGathererConfig,
    ) -> Self {
        Self { tx, config }
    }
}

#[async_trait]
impl Gatherer for OpenFoodFactsGatherer {
    async fn gather(mut self) -> Result<usize, errors::ProcessingError> {
        Ok(open_food_facts::reader::load(
            self.config.open_food_facts_path,
            move |headers: csv::StringRecord, record: csv::StringRecord| {
                let tx = self.tx.clone();
                async move {
                    if let Err(err) = tx.send(OpenFoodFactsRunnerMessage { record, headers }).await
                    {
                        log::error!("Failed to send message over channel: {err}");
                    };
                }
            },
        )
        .await?)
    }
}

/// Feeds the processor with Open Food Facts data.
#[must_use]
#[derive(Debug, Clone)]
pub struct OpenFoodFactsForwarder<P>
where
    P: OpenFoodFactsProcessor,
{
    rx: async_channel::Receiver<OpenFoodFactsRunnerMessage>,
    phantom: std::marker::PhantomData<P>,
}

impl<P> OpenFoodFactsForwarder<P>
where
    P: OpenFoodFactsProcessor,
{
    pub fn new(rx: async_channel::Receiver<OpenFoodFactsRunnerMessage>) -> Self {
        Self { rx, phantom: std::marker::PhantomData }
    }

    /// Handles a message from the `gather`.
    async fn forward(
        self,
        processor: P,
        mut collector: P::Collector,
        sources: std::sync::Arc<P::Sources>,
        config: P::Config,
    ) -> P::Collector {
        while let Ok(msg) = self.rx.recv().await {
            let result: csv::Result<open_food_facts::data::Record> =
                msg.record.deserialize(Some(&msg.headers));
            match result {
                Ok(record) => {
                    if let Err(err) = processor.process_open_food_facts_record(
                        record,
                        sources.borrow(),
                        &mut collector,
                        &config,
                    ) {
                        log::error!("Failed to handle an Open Food Facts record: {}", err);
                    }
                }
                Err(err) => {
                    log::error!(
                        "Failed to parse an Open Food Facts record: {} \nRecord:\n{:?}\nHeaders:\n{:?}\n\n",
                        err,
                        msg.record,
                        msg.headers,
                    );
                }
            }
        }
        collector
    }
}

#[async_trait]
impl<P> Forwarder<P> for OpenFoodFactsForwarder<P>
where
    P: OpenFoodFactsProcessor,
{
    async fn spawn(
        self,
        pool: &mut future_pool::FuturePool<P::Collector>,
        processor: P,
        collector: P::Collector,
        sources: std::sync::Arc<P::Sources>,
        config: P::Config,
    ) {
        pool.spawn(self.forward(processor, collector, sources, config));
    }
}

/// Orchiestrates processing of Open Food Facts data.
#[derive(Debug)]
pub struct OpenFoodFactsRunner<P>
where
    P: OpenFoodFactsProcessor,
{
    phantom: std::marker::PhantomData<P>,
}

#[async_trait]
impl<P> Runnable<P> for OpenFoodFactsRunner<P>
where
    P: OpenFoodFactsProcessor + 'static,
    for<'a> config::OpenFoodFactsGathererConfig: From<&'a <P as Processor>::Config>,
    for<'a> <P::Sources as Sourceable>::Config: From<&'a <P as Processor>::Config>,
{
    type Config = config::OpenFoodFactsGathererConfig;
    type Gatherer = OpenFoodFactsGatherer;
    type Forwarder = OpenFoodFactsForwarder<P>;

    fn create(
        config: Self::Config,
    ) -> Result<(Self::Gatherer, Self::Forwarder), errors::ProcessingError> {
        const CHANNEL_QUEUE_BOUND: usize = 10;
        let (tx, rx) = async_channel::bounded(CHANNEL_QUEUE_BOUND);
        let gatherer = OpenFoodFactsGatherer::new(tx, config);
        let forwarder = OpenFoodFactsForwarder::new(rx);
        Ok((gatherer, forwarder))
    }
}

/// Processor for the `EuEcolabelRunner`.
pub trait EuEcolabelProcessor: Processor {
    /// Handles one `EuEcolabel` record.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the processing of the record fails in any way.
    fn process_eu_ecolabel_record(
        &self,
        record: eu_ecolabel::data::Record,
        sources: &Self::Sources,
        collector: &mut Self::Collector,
        config: &Self::Config,
    ) -> Result<(), errors::ProcessingError>;
}

/// Message send throught `OpenFoodFactsRunner` channel.
#[derive(Clone, Debug)]
pub struct EuEcolabelRunnerMessage {
    record: csv::StringRecord,
    headers: csv::StringRecord,
}

/// Async reader for EU Ecolabel data.
#[must_use]
#[derive(Debug)]
pub struct EuEcolabelGatherer {
    tx: async_channel::Sender<EuEcolabelRunnerMessage>,
    config: config::EuEcolabelGathererConfig,
}

impl EuEcolabelGatherer {
    pub fn new(
        tx: async_channel::Sender<EuEcolabelRunnerMessage>,
        config: config::EuEcolabelGathererConfig,
    ) -> Self {
        Self { tx, config }
    }
}

#[async_trait]
impl Gatherer for EuEcolabelGatherer {
    async fn gather(mut self) -> Result<usize, errors::ProcessingError> {
        Ok(eu_ecolabel::reader::load(
            self.config.eu_ecolabel_path,
            move |headers: csv::StringRecord, record: csv::StringRecord| {
                let tx = self.tx.clone();
                async move {
                    if let Err(err) = tx.send(EuEcolabelRunnerMessage { record, headers }).await {
                        log::error!("Failed to send message over channel: {err}");
                    };
                }
            },
        )
        .await?)
    }
}

/// Feeds the processor with EU Ecolabel data.
#[must_use]
#[derive(Debug, Clone)]
pub struct EuEcolabelForwarder<P>
where
    P: EuEcolabelProcessor,
{
    rx: async_channel::Receiver<EuEcolabelRunnerMessage>,
    phantom: std::marker::PhantomData<P>,
}

impl<P> EuEcolabelForwarder<P>
where
    P: EuEcolabelProcessor,
{
    pub fn new(rx: async_channel::Receiver<EuEcolabelRunnerMessage>) -> Self {
        Self { rx, phantom: std::marker::PhantomData }
    }

    /// Handles a message from the `gather`.
    async fn forward(
        self,
        processor: P,
        mut collector: P::Collector,
        sources: std::sync::Arc<P::Sources>,
        config: P::Config,
    ) -> P::Collector {
        while let Ok(msg) = self.rx.recv().await {
            let result: csv::Result<eu_ecolabel::data::Record> =
                msg.record.deserialize(Some(&msg.headers));
            match result {
                Ok(record) => {
                    if let Err(err) = processor.process_eu_ecolabel_record(
                        record,
                        sources.borrow(),
                        &mut collector,
                        &config,
                    ) {
                        log::error!("Failed to handle an EU Ecolabel record: {}", err);
                    }
                }
                Err(err) => {
                    log::error!(
                        "Failed to parse an EU Ecolabel record: {} \nRecord:\n{:?}\nHeaders:\n{:?}\n\n",
                        err,
                        msg.record,
                        msg.headers,
                    );
                }
            }
        }
        collector
    }
}

#[async_trait]
impl<P> Forwarder<P> for EuEcolabelForwarder<P>
where
    P: EuEcolabelProcessor,
{
    async fn spawn(
        self,
        pool: &mut future_pool::FuturePool<P::Collector>,
        processor: P,
        collector: P::Collector,
        sources: std::sync::Arc<P::Sources>,
        config: P::Config,
    ) {
        pool.spawn(self.forward(processor, collector, sources, config));
    }
}

/// Orchiestrates processing of EU Ecolabel data.
#[derive(Debug)]
pub struct EuEcolabelRunner<P>
where
    P: EuEcolabelProcessor,
{
    phantom: std::marker::PhantomData<P>,
}

#[async_trait]
impl<P> Runnable<P> for EuEcolabelRunner<P>
where
    P: EuEcolabelProcessor + 'static,
    for<'a> config::EuEcolabelGathererConfig: From<&'a <P as Processor>::Config>,
    for<'a> <P::Sources as Sourceable>::Config: From<&'a <P as Processor>::Config>,
{
    type Config = config::EuEcolabelGathererConfig;
    type Gatherer = EuEcolabelGatherer;
    type Forwarder = EuEcolabelForwarder<P>;

    fn create(
        config: Self::Config,
    ) -> Result<(Self::Gatherer, Self::Forwarder), errors::ProcessingError> {
        const CHANNEL_QUEUE_BOUND: usize = 10;
        let (tx, rx) = async_channel::bounded(CHANNEL_QUEUE_BOUND);
        let gatherer = EuEcolabelGatherer::new(tx, config);
        let forwarder = EuEcolabelForwarder::new(rx);
        Ok((gatherer, forwarder))
    }
}

/// Async reader for
///  - Wikidata
///  - Open Food Facts
///  - EU Ecolabel
/// data.
#[derive(Debug)]
pub struct FullGatherer {
    wiki: WikidataGatherer,
    off: OpenFoodFactsGatherer,
    eu_ecolabel: EuEcolabelGatherer,
}

#[async_trait]
impl Gatherer for FullGatherer {
    async fn gather(mut self) -> Result<usize, errors::ProcessingError> {
        Ok([
            tokio::spawn(self.wiki.gather()).await??,
            tokio::spawn(self.off.gather()).await??,
            tokio::spawn(self.eu_ecolabel.gather()).await??,
        ]
        .iter()
        .sum())
    }
}

/// Feeds the processor with
///  - Wikidata
///  - Open Food Facts
///  - EU Ecolabel
/// data.
#[derive(Debug, Clone)]
pub struct FullForwarder<P>
where
    P: WikidataProcessor + OpenFoodFactsProcessor + EuEcolabelProcessor,
{
    wiki: WikidataForwarder<P>,
    off: OpenFoodFactsForwarder<P>,
    eu_ecolabel: EuEcolabelForwarder<P>,
}

#[async_trait]
impl<P> Forwarder<P> for FullForwarder<P>
where
    P: WikidataProcessor + OpenFoodFactsProcessor + EuEcolabelProcessor,
{
    async fn spawn(
        self,
        pool: &mut future_pool::FuturePool<P::Collector>,
        processor: P,
        collector: P::Collector,
        sources: std::sync::Arc<P::Sources>,
        config: P::Config,
    ) {
        self.wiki
            .spawn(pool, processor.clone(), collector.clone(), sources.clone(), config.clone())
            .await;
        self.off
            .spawn(pool, processor.clone(), collector.clone(), sources.clone(), config.clone())
            .await;
        self.eu_ecolabel.spawn(pool, processor, collector, sources, config).await;
    }
}

/// Orchiestrates processing of
///  - Wikidata
///  - Open Food Facts
///  - EU Ecolabel
/// data.
#[derive(Debug)]
pub struct FullRunner<P>
where
    P: WikidataProcessor + OpenFoodFactsProcessor + EuEcolabelProcessor,
{
    phantom: std::marker::PhantomData<P>,
}

#[async_trait]
impl<P> Runnable<P> for FullRunner<P>
where
    P: WikidataProcessor + OpenFoodFactsProcessor + EuEcolabelProcessor + 'static,
    for<'a> config::FullGathererConfig: From<&'a <P as Processor>::Config>,
    for<'a> config::WikidataGathererConfig: From<&'a <P as Processor>::Config>,
    for<'a> config::OpenFoodFactsGathererConfig: From<&'a <P as Processor>::Config>,
    for<'a> config::EuEcolabelGathererConfig: From<&'a <P as Processor>::Config>,
    for<'a> <P::Sources as Sourceable>::Config: From<&'a <P as Processor>::Config>,
{
    type Config = config::FullGathererConfig;
    type Gatherer = FullGatherer;
    type Forwarder = FullForwarder<P>;

    fn create(
        config: Self::Config,
    ) -> Result<(Self::Gatherer, Self::Forwarder), errors::ProcessingError> {
        let (wiki_gatherer, wiki_forwarder) = WikidataRunner::<P>::create((&config).into())?;
        let (off_gatherer, off_forwarder) = OpenFoodFactsRunner::<P>::create((&config).into())?;
        let (eu_ecolabel_gatherer, eu_ecolabel_forwarder) =
            EuEcolabelRunner::<P>::create((&config).into())?;
        let gatherer = FullGatherer {
            wiki: wiki_gatherer,
            off: off_gatherer,
            eu_ecolabel: eu_ecolabel_gatherer,
        };
        let forwarder = FullForwarder {
            wiki: wiki_forwarder,
            off: off_forwarder,
            eu_ecolabel: eu_ecolabel_forwarder,
        };
        Ok((gatherer, forwarder))
    }
}
