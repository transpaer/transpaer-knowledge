use std::borrow::Borrow;

use async_trait::async_trait;

use sustainity_collecting::{eu_ecolabel, open_food_facts};
use sustainity_wikidata::data::Entity;

use crate::{
    config, errors,
    processing::{Processor, Runnable, Sourceable},
};

/// Processor for the `WikidataRunner`.
pub trait WikidataProcessor: Processor {
    /// Handles one Wikidata entity.
    fn handle_wikidata_entity(
        &self,
        msg: &str,
        entity: Entity,
        sources: &Self::Sources,
        collector: &mut Self::Collector,
        config: &Self::Config,
    ) -> Result<(), errors::ProcessingError>;
}

/// Feeds the implementaiotn with dat comming from Wikidata data set.
#[derive(Debug)]
pub struct WikidataRunner<P>
where
    P: WikidataProcessor,
{
    /// Wikidata dump file loader.
    wiki: sustainity_wikidata::dump::Loader,

    phantom: std::marker::PhantomData<P>,
}

#[async_trait]
impl<P> Runnable<P> for WikidataRunner<P>
where
    P: WikidataProcessor + 'static,
    for<'a> config::WikidataRunnerConfig: From<&'a <P as Processor>::Config>,
    for<'a> <P::Sources as Sourceable>::Config: From<&'a <P as Processor>::Config>,
{
    type Config = config::WikidataRunnerConfig;
    type Message = String;

    fn load(config: Self::Config) -> Result<Self, errors::ProcessingError> {
        Ok(Self {
            wiki: sustainity_wikidata::dump::Loader::load(&config.wikidata_path)?,
            phantom: std::marker::PhantomData,
        })
    }

    async fn gather(
        mut self,
        tx: async_channel::Sender<Self::Message>,
        _config: Self::Config,
    ) -> Result<usize, errors::ProcessingError> {
        Ok(self
            .wiki
            .run(move |s: String| {
                let tx2 = tx.clone();
                async move {
                    if let Err(err) = tx2.send(s).await {
                        log::error!("Failed to send message over channel: {err}");
                    }
                }
            })
            .await?)
    }

    /// Handles a message from `gather`.
    async fn process(
        rx: async_channel::Receiver<Self::Message>,
        processor: P,
        sources: std::sync::Arc<P::Sources>,
        config: P::Config,
    ) -> P::Collector {
        let mut collector = P::Collector::default();
        if let Err(err) = processor.initialize(&mut collector, sources.borrow(), &config) {
            log::error!("Failed to initialize the processor: {}", err);
        }
        while let Ok(msg) = rx.recv().await {
            let result: Result<Entity, serde_json::Error> = serde_json::from_str(&msg);
            match result {
                Ok(entity) => {
                    if let Err(err) = processor.handle_wikidata_entity(
                        &msg,
                        entity,
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
}

/// Processor for the `FullRunner`.
pub trait FullProcessor: Processor {
    /// Handles one Wikidata entity.
    fn handle_wikidata_entity(
        &self,
        msg: &str,
        entity: Entity,
        sources: &Self::Sources,
        collector: &mut Self::Collector,
        config: &Self::Config,
    ) -> Result<(), errors::ProcessingError>;

    /// Handles one Open Food Facts record.
    fn handle_open_food_facts_record(
        &self,
        record: open_food_facts::data::Record,
        sources: &Self::Sources,
        collector: &mut Self::Collector,
        config: &Self::Config,
    ) -> Result<(), errors::ProcessingError>;

    /// Handles one EU Ecolabel record.
    fn handle_eu_ecolabel_record(
        &self,
        record: eu_ecolabel::data::Record,
        sources: &Self::Sources,
        collector: &mut Self::Collector,
        config: &Self::Config,
    ) -> Result<(), errors::ProcessingError>;
}

/// Message send throught `FullRunner` channel.
#[derive(Clone, Debug)]
pub enum FullRunnerMessage {
    Wikidata(String),
    OpenFoodFacts { record: csv::StringRecord, headers: csv::StringRecord },
    EuEcolabel { record: csv::StringRecord, headers: csv::StringRecord },
}

/// Feeds the implementaiotn with dat comming from
///  - Wikidata
///  - Open Food Facts
///  - EU Ecolabel
/// data sets.
#[derive(Debug)]
pub struct FullRunner<P>
where
    P: FullProcessor,
{
    /// Wikidata dump file loader.
    wiki: sustainity_wikidata::dump::Loader,

    phantom: std::marker::PhantomData<P>,
}

#[async_trait]
impl<P> Runnable<P> for FullRunner<P>
where
    P: FullProcessor + 'static,
    for<'a> config::FullRunnerConfig: From<&'a <P as Processor>::Config>,
    for<'a> <P::Sources as Sourceable>::Config: From<&'a <P as Processor>::Config>,
{
    type Config = config::FullRunnerConfig;
    type Message = FullRunnerMessage;

    fn load(config: Self::Config) -> Result<Self, errors::ProcessingError> {
        Ok(Self {
            wiki: sustainity_wikidata::dump::Loader::load(&config.wikidata_path)?,
            phantom: std::marker::PhantomData,
        })
    }

    async fn gather(
        mut self,
        tx: async_channel::Sender<Self::Message>,
        config: Self::Config,
    ) -> Result<usize, errors::ProcessingError> {
        let wiki_tx = tx.clone();
        let handle1 = tokio::spawn(self.wiki.run(move |msg: String| {
            let tx = wiki_tx.clone();
            async move {
                if let Err(err) = tx.send(FullRunnerMessage::Wikidata(msg)).await {
                    log::error!("Failed to send message over channel: {err}");
                };
            }
        }));

        let eu_tx = tx.clone();
        let handle2 = tokio::spawn(eu_ecolabel::reader::load(
            config.eu_ecolabel_path,
            move |headers: csv::StringRecord, record: csv::StringRecord| {
                let tx = eu_tx.clone();
                async move {
                    if let Err(err) =
                        tx.send(FullRunnerMessage::EuEcolabel { headers, record }).await
                    {
                        log::error!("Failed to send message over channel: {err}");
                    };
                }
            },
        ));

        let off_tx = tx.clone();
        let handle3 = tokio::spawn(open_food_facts::reader::load(
            config.open_food_facts_path,
            move |headers: csv::StringRecord, record: csv::StringRecord| {
                let tx = off_tx.clone();
                async move {
                    if let Err(err) =
                        tx.send(FullRunnerMessage::OpenFoodFacts { headers, record }).await
                    {
                        log::error!("Failed to send message over channel: {err}");
                    };
                }
            },
        ));

        Ok([handle1.await??, handle2.await??, handle3.await??].iter().sum())
    }

    /// Handles a message from `gather`.
    async fn process(
        rx: async_channel::Receiver<Self::Message>,
        processor: P,
        sources: std::sync::Arc<P::Sources>,
        config: P::Config,
    ) -> P::Collector {
        let mut collector = P::Collector::default();
        if let Err(err) = processor.initialize(&mut collector, sources.borrow(), &config) {
            log::error!("Failed to initialize the processor: {}", err);
        }
        while let Ok(msg) = rx.recv().await {
            match msg {
                FullRunnerMessage::Wikidata(msg) => {
                    let result: Result<Entity, serde_json::Error> = serde_json::from_str(&msg);
                    match result {
                        Ok(entity) => {
                            if let Err(err) = processor.handle_wikidata_entity(
                                &msg,
                                entity,
                                sources.borrow(),
                                &mut collector,
                                &config,
                            ) {
                                log::error!("Failed to handle an entity: {}", err);
                            }
                        }
                        Err(err) => {
                            log::error!(
                                "Failed to parse an entity: {} \nMessage:\n'{}'\n\n",
                                err,
                                msg
                            );
                        }
                    }
                }
                FullRunnerMessage::OpenFoodFacts { record, headers } => {
                    let result: csv::Result<open_food_facts::data::Record> =
                        record.deserialize(Some(&headers));
                    match result {
                        Ok(record) => {
                            if let Err(err) = processor.handle_open_food_facts_record(
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
                                record,
                                headers
                            );
                        }
                    }
                }
                FullRunnerMessage::EuEcolabel { record, headers } => {
                    let result: csv::Result<eu_ecolabel::data::Record> =
                        record.deserialize(Some(&headers));
                    match result {
                        Ok(record) => {
                            if let Err(err) = processor.handle_eu_ecolabel_record(
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
                                record,
                                headers
                            );
                        }
                    }
                }
            }
        }
        collector
    }
}
