// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// TODO: There is a lot of dead code here. Maybe time for cleanup?

use async_trait::async_trait;

use transpaer_collecting::{eu_ecolabel, open_food_facts};

use crate::{
    config, errors,
    parallel::{self, Consumer, Flow, Processor, Producer, Sender},
};

pub trait Stash: Send {
    type Input: Clone + Send;

    fn stash(&mut self, input: Self::Input) -> Result<(), errors::ProcessingError>;

    fn finish(self) -> Result<(), errors::ProcessingError>;
}

#[derive(Clone, Debug, Default)]
pub struct RunnerConsumer<S>
where
    S: Stash,
{
    stash: S,
}

impl<S> RunnerConsumer<S>
where
    S: Stash,
{
    pub fn new(stash: S) -> Self {
        Self { stash }
    }
}

#[async_trait]
impl<S> Consumer for RunnerConsumer<S>
where
    S: Stash,
{
    type Input = S::Input;
    type Error = errors::ProcessingError;

    async fn consume(&mut self, entry: Self::Input) -> Result<(), Self::Error> {
        self.stash.stash(entry)
    }

    async fn finish(self) -> Result<(), Self::Error> {
        self.stash.finish()
    }
}

/// Implementation of `Producer` trait for Wikidata data.
#[must_use]
#[derive(Debug)]
pub struct WikidataProducer {
    wiki: transpaer_wikidata::dump::Loader,
}

impl WikidataProducer {
    /// Constructs a new `WikidataProducer`
    pub fn new(config: &config::WikidataProducerConfig) -> Result<Self, errors::ProcessingError> {
        Ok(Self { wiki: transpaer_wikidata::dump::Loader::load(&config.wikidata_path)? })
    }
}

#[async_trait]
impl Producer for WikidataProducer {
    type Output = String;
    type Error = errors::ProcessingError;

    async fn produce(self, tx: Sender<Self::Output>) -> Result<(), errors::ProcessingError> {
        let num = self
            .wiki
            .run(move |s: String| {
                let tx2 = tx.clone();
                async move {
                    tx2.send(s).await;
                }
            })
            .await?;

        log::info!("Read {num} Wikidata entries");
        Ok(())
    }
}

#[async_trait]
pub trait WikidataWorker: Clone + Send {
    type Output: Clone + Send;

    /// Handles one Wikidata entity.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the processing of the entry fails in any way.
    async fn process(
        &mut self,
        msg: &str,
        entity: transpaer_wikidata::data::Entity,
        tx: parallel::Sender<Self::Output>,
    ) -> Result<(), errors::ProcessingError>;

    async fn finish(
        self,
        tx: parallel::Sender<Self::Output>,
    ) -> Result<(), errors::ProcessingError>;
}

#[derive(Clone, Debug)]
pub struct WikidataProcessor<W>
where
    W: WikidataWorker,
{
    worker: W,
}

impl<W> WikidataProcessor<W>
where
    W: WikidataWorker,
{
    pub fn new(worker: W) -> Self {
        Self { worker }
    }
}

#[async_trait]
impl<W> Processor for WikidataProcessor<W>
where
    W: WikidataWorker + Sync,
{
    type Input = String;
    type Output = W::Output;
    type Error = errors::ProcessingError;

    async fn process(
        &mut self,
        input: Self::Input,
        tx: Sender<Self::Output>,
    ) -> Result<(), Self::Error> {
        let result: Result<transpaer_wikidata::data::Entity, serde_json::Error> =
            serde_json::from_str(&input);
        match result {
            Ok(entity) => {
                self.worker.process(&input, entity, tx).await?;
            }
            Err(err) => {
                log::error!("Failed to parse a Wikidata entity: {err} \nMessage:\n'{input}'\n\n",);
            }
        }
        Ok(())
    }

    async fn finish(self, tx: Sender<Self::Output>) -> Result<(), Self::Error> {
        self.worker.finish(tx).await
    }
}

pub struct WikidataRunner<W, S, C>
where
    W: WikidataWorker,
    S: Stash,
    C: Clone + Send,
{
    phantom: std::marker::PhantomData<(W, S, C)>,
}

impl<W, S, C> WikidataRunner<W, S, C>
where
    W: WikidataWorker + Sync + 'static,
    S: Stash<Input = W::Output> + 'static,
    C: Clone + Send,
    for<'c> &'c C: Into<config::WikidataProducerConfig>,
{
    pub fn flow(
        flow: Flow,
        config: &C,
        worker: W,
        stash: S,
    ) -> Result<Flow, errors::ProcessingError> {
        let (tx1, rx1) = parallel::bounded::<String>();
        let (tx2, rx2) = parallel::bounded::<W::Output>();

        let producer = WikidataProducer::new(&config.into())?;
        let processor = WikidataProcessor::new(worker);
        let consumer = RunnerConsumer::new(stash);

        let flow = flow
            .name("wiki")
            .spawn_producer(producer, tx1)?
            .spawn_processors(processor, rx1, tx2)?
            .spawn_consumer(consumer, rx2)?;

        Ok(flow)
    }
}

/// Message send throught `OpenFoodFactsRunner` channel.
#[derive(Clone, Debug)]
pub struct OpenFoodFactsRunnerMessage {
    record: csv::StringRecord,
    headers: csv::StringRecord,
}

/// Implementation of `Producer` trait for Open Food Facts data.
#[must_use]
#[derive(Debug)]
pub struct OpenFoodFactsProducer {
    config: config::OpenFoodFactsProducerConfig,
}

impl OpenFoodFactsProducer {
    /// Constructs a new `OpenFoodFactsProducer`
    #[allow(clippy::unnecessary_wraps)]
    pub fn new(
        config: config::OpenFoodFactsProducerConfig,
    ) -> Result<Self, errors::ProcessingError> {
        Ok(Self { config })
    }
}

#[async_trait]
impl Producer for OpenFoodFactsProducer {
    type Output = OpenFoodFactsRunnerMessage;
    type Error = errors::ProcessingError;

    async fn produce(self, tx: Sender<Self::Output>) -> Result<(), errors::ProcessingError> {
        let num = open_food_facts::reader::load(
            self.config.open_food_facts_path,
            move |headers: csv::StringRecord, record: csv::StringRecord| {
                let tx2 = tx.clone();
                async move {
                    tx2.send(OpenFoodFactsRunnerMessage { record, headers }).await;
                }
            },
        )
        .await?;

        log::info!("Read {num} Open Food Facts records");
        Ok(())
    }
}

#[async_trait]
pub trait OpenFoodFactsWorker: Clone + Send {
    type Output: Clone + Send;

    /// Handles one Open Food Facts record.
    ///
    /// # Errors
    ///
    /// Returns `Err` if processing of the record fails in any way.
    async fn process(
        &mut self,
        record: open_food_facts::data::Record,
        tx: parallel::Sender<Self::Output>,
    ) -> Result<(), errors::ProcessingError>;

    async fn finish(
        self,
        tx: parallel::Sender<Self::Output>,
    ) -> Result<(), errors::ProcessingError>;
}

#[derive(Clone, Debug)]
pub struct OpenFoodFactsProcessor<W>
where
    W: OpenFoodFactsWorker,
{
    worker: W,
}

impl<W> OpenFoodFactsProcessor<W>
where
    W: OpenFoodFactsWorker,
{
    pub fn new(worker: W) -> Self {
        Self { worker }
    }
}

#[async_trait]
impl<W> Processor for OpenFoodFactsProcessor<W>
where
    W: OpenFoodFactsWorker + Sync,
{
    type Input = OpenFoodFactsRunnerMessage;
    type Output = W::Output;
    type Error = errors::ProcessingError;

    async fn process(
        &mut self,
        input: Self::Input,
        tx: Sender<Self::Output>,
    ) -> Result<(), Self::Error> {
        let result: csv::Result<open_food_facts::data::Record> =
            input.record.deserialize(Some(&input.headers));
        match result {
            Ok(record) => {
                self.worker.process(record, tx).await?;
            }
            Err(err) => {
                log::error!(
                    "Failed to parse an Open Food Facts record: {}\nRecord:\n{:?}\nHeaders:\n{:?}\n\n",
                    err,
                    input.record,
                    input.headers,
                );
            }
        }
        Ok(())
    }

    async fn finish(self, tx: Sender<Self::Output>) -> Result<(), Self::Error> {
        self.worker.finish(tx).await
    }
}

#[allow(dead_code)]
pub struct OpenFoodFactsRunner<W, S, C>
where
    W: OpenFoodFactsWorker,
    S: Stash,
    C: Clone + Send,
{
    phantom: std::marker::PhantomData<(W, S, C)>,
}

#[allow(dead_code)]
impl<W, S, C> OpenFoodFactsRunner<W, S, C>
where
    W: OpenFoodFactsWorker + Sync + 'static,
    S: Stash<Input = W::Output> + 'static,
    C: Clone + Send,
    for<'c> &'c C: Into<config::OpenFoodFactsProducerConfig>,
{
    pub fn flow(
        flow: Flow,
        config: &C,
        worker: W,
        stash: S,
    ) -> Result<Flow, errors::ProcessingError> {
        let (tx1, rx1) = parallel::bounded::<OpenFoodFactsRunnerMessage>();
        let (tx2, rx2) = parallel::bounded::<W::Output>();

        let producer = OpenFoodFactsProducer::new(config.into())?;
        let processor = OpenFoodFactsProcessor::new(worker);
        let consumer = RunnerConsumer::new(stash);

        let flow = flow
            .name("off")
            .spawn_producer(producer, tx1)?
            .spawn_processors(processor, rx1, tx2)?
            .spawn_consumer(consumer, rx2)?;

        Ok(flow)
    }
}

/// Message send throught `EuEcolabelRunner` channel.
#[derive(Clone, Debug)]
pub struct EuEcolabelRunnerMessage {
    record: csv::StringRecord,
    headers: csv::StringRecord,
}

/// Implementation of `Producer` trait for EU Ecolabel data.
#[must_use]
#[derive(Debug)]
pub struct EuEcolabelProducer {
    config: config::EuEcolabelProducerConfig,
}

impl EuEcolabelProducer {
    /// Constructs a new `EuEcolabelProducer`.
    #[allow(clippy::unnecessary_wraps)]
    pub fn new(config: config::EuEcolabelProducerConfig) -> Result<Self, errors::ProcessingError> {
        Ok(Self { config })
    }
}

#[async_trait]
impl Producer for EuEcolabelProducer {
    type Output = EuEcolabelRunnerMessage;
    type Error = errors::ProcessingError;

    async fn produce(self, tx: Sender<Self::Output>) -> Result<(), errors::ProcessingError> {
        let num = eu_ecolabel::reader::load(
            &self.config.eu_ecolabel_path,
            move |headers: csv::StringRecord, record: csv::StringRecord| {
                let tx2 = tx.clone();
                async move {
                    tx2.send(EuEcolabelRunnerMessage { record, headers }).await;
                }
            },
        )
        .await?;

        log::info!("Read {num} EU Ecolabel records");
        Ok(())
    }
}

#[async_trait]
pub trait EuEcolabelWorker: Clone + Send {
    type Output: Clone + Send;

    /// Handles one Open Food Facts record.
    ///
    /// # Errors
    ///
    /// Returns `Err` if processing of the record fails in any way.
    async fn process(
        &mut self,
        record: eu_ecolabel::data::Record,
        tx: parallel::Sender<Self::Output>,
    ) -> Result<(), errors::ProcessingError>;

    async fn finish(
        self,
        tx: parallel::Sender<Self::Output>,
    ) -> Result<(), errors::ProcessingError>;
}

#[derive(Clone, Debug)]
pub struct EuEcolabelProcessor<W>
where
    W: EuEcolabelWorker,
{
    worker: W,
}

impl<W> EuEcolabelProcessor<W>
where
    W: EuEcolabelWorker,
{
    pub fn new(worker: W) -> Self {
        Self { worker }
    }
}

#[async_trait]
impl<W> Processor for EuEcolabelProcessor<W>
where
    W: EuEcolabelWorker + Sync,
{
    type Input = EuEcolabelRunnerMessage;
    type Output = W::Output;
    type Error = errors::ProcessingError;

    async fn process(
        &mut self,
        input: Self::Input,
        tx: Sender<Self::Output>,
    ) -> Result<(), Self::Error> {
        let result: csv::Result<eu_ecolabel::data::Record> =
            input.record.deserialize(Some(&input.headers));
        match result {
            Ok(record) => {
                self.worker.process(record, tx).await?;
            }
            Err(err) => {
                log::error!(
                    "Failed to parse an Open Food Facts record: {}\nRecord:\n{:?}\nHeaders:\n{:?}\n\n",
                    err,
                    input.record,
                    input.headers,
                );
            }
        }
        Ok(())
    }

    async fn finish(self, tx: Sender<Self::Output>) -> Result<(), Self::Error> {
        self.worker.finish(tx).await
    }
}

#[allow(dead_code)]
pub struct EuEcolabelRunner<W, S, C>
where
    W: EuEcolabelWorker,
    S: Stash,
    C: Clone + Send,
{
    phantom: std::marker::PhantomData<(W, S, C)>,
}

#[allow(dead_code)]
impl<W, S, C> EuEcolabelRunner<W, S, C>
where
    W: EuEcolabelWorker + Sync + 'static,
    S: Stash<Input = W::Output> + 'static,
    C: Clone + Send,
    for<'c> &'c C: Into<config::EuEcolabelProducerConfig>,
{
    pub fn flow(
        flow: Flow,
        config: &C,
        worker: W,
        stash: S,
    ) -> Result<Flow, errors::ProcessingError> {
        let (tx1, rx1) = parallel::bounded::<EuEcolabelRunnerMessage>();
        let (tx2, rx2) = parallel::bounded::<W::Output>();

        let producer = EuEcolabelProducer::new(config.into())?;
        let processor = EuEcolabelProcessor::<W>::new(worker);
        let consumer = RunnerConsumer::<S>::new(stash);

        let flow = flow
            .name("eu")
            .spawn_producer(producer, tx1)?
            .spawn_processors(processor, rx1, tx2)?
            .spawn_consumer(consumer, rx2)?;

        Ok(flow)
    }
}

#[allow(dead_code)]
pub struct FullRunner<W, S, C>
where
    W: WikidataWorker + OpenFoodFactsWorker + EuEcolabelWorker,
    S: Stash,
    C: Clone + Send,
{
    phantom: std::marker::PhantomData<(W, S, C)>,
}

#[allow(dead_code)]
impl<W, S, C> FullRunner<W, S, C>
where
    S: Stash + 'static,
    W: WikidataWorker<Output = S::Input>
        + OpenFoodFactsWorker<Output = S::Input>
        + EuEcolabelWorker<Output = S::Input>
        + Sync
        + 'static,
    C: Clone + Send,
    for<'c> &'c C: Into<config::WikidataProducerConfig>
        + Into<config::OpenFoodFactsProducerConfig>
        + Into<config::EuEcolabelProducerConfig>,
{
    pub fn flow(
        flow: Flow,
        config: &C,
        worker: &W,
        stash: S,
    ) -> Result<Flow, errors::ProcessingError> {
        let (wiki_tx, wiki_rx) = parallel::bounded::<String>();
        let (off_tx, off_rx) = parallel::bounded::<OpenFoodFactsRunnerMessage>();
        let (eu_tx, eu_rx) = parallel::bounded::<EuEcolabelRunnerMessage>();
        let (consumer_tx, consumer_rx) = parallel::bounded::<S::Input>();

        let wiki_producer = WikidataProducer::new(&config.into())?;
        let wiki_processor = WikidataProcessor::<W>::new(worker.clone());
        let off_producer = OpenFoodFactsProducer::new(config.into())?;
        let off_processor = OpenFoodFactsProcessor::<W>::new(worker.clone());
        let eu_producer = EuEcolabelProducer::new(config.into())?;
        let eu_processor = EuEcolabelProcessor::<W>::new(worker.clone());
        let consumer = RunnerConsumer::<S>::new(stash);

        let flow = flow
            .name("wiki")
            .spawn_producer(wiki_producer, wiki_tx)?
            .spawn_processors(wiki_processor, wiki_rx, consumer_tx.clone())?
            .name("off")
            .spawn_producer(off_producer, off_tx)?
            .spawn_processors(off_processor, off_rx, consumer_tx.clone())?
            .name("eu")
            .spawn_producer(eu_producer, eu_tx)?
            .spawn_processors(eu_processor, eu_rx, consumer_tx)?
            .name("stash")
            .spawn_consumer(consumer, consumer_rx)?;

        Ok(flow)
    }
}
