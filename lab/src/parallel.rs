use async_trait::async_trait;

use crate::errors;

const CHANNEL_CAP: usize = 100;

#[derive(Clone)]
pub struct Sender<T>
where
    T: Clone + Send,
{
    sender: async_channel::Sender<T>,
}

impl<T> Sender<T>
where
    T: Clone + Send,
{
    pub async fn send(&self, message: T) {
        if let Err(err) = self.sender.send(message).await {
            log::error!("Flow sender: {err}");
        }
    }
}

pub enum Recv<T>
where
    T: Clone + Send,
{
    Value(T),
    Closed,
}

#[derive(Clone)]
pub struct Receiver<T>
where
    T: Clone + Send,
{
    receiver: async_channel::Receiver<T>,
}

impl<T> Receiver<T>
where
    T: Clone + Send,
{
    pub async fn recv(&self) -> Recv<T> {
        match self.receiver.recv().await {
            Ok(value) => Recv::Value(value),
            Err(_) => Recv::Closed,
        }
    }
}

#[must_use]
pub fn bounded<T>() -> (Sender<T>, Receiver<T>)
where
    T: Clone + Send,
{
    let (sender, receiver) = async_channel::bounded(CHANNEL_CAP);
    (Sender { sender }, Receiver { receiver })
}

#[async_trait]
pub trait Producer: Send + Sync {
    type Output: Clone + Send;
    type Error: std::error::Error;

    async fn produce(self, tx: Sender<Self::Output>) -> Result<(), Self::Error>;
}

#[async_trait]
pub trait RefProducer: Send + Sync {
    type Output: Clone + Send;
    type Error: std::error::Error;

    async fn produce(&self, tx: Sender<Self::Output>) -> Result<(), Self::Error>;
}

#[async_trait]
pub trait Processor: Clone + Send {
    type Input: Clone + Send;
    type Output: Clone + Send;
    type Error: std::error::Error;

    async fn process(
        &mut self,
        input: Self::Input,
        tx: Sender<Self::Output>,
    ) -> Result<(), Self::Error>;

    async fn finish(self, tx: Sender<Self::Output>) -> Result<(), Self::Error>;
}

#[async_trait]
pub trait Consumer: Send {
    type Input: Clone + Send;
    type Error: std::error::Error;

    async fn consume(&mut self, input: Self::Input) -> Result<(), Self::Error>;

    async fn finish(self) -> Result<(), Self::Error>;
}

#[derive(Debug, Default)]
pub struct Flow {
    name: Option<String>,
    handlers: Vec<std::thread::JoinHandle<()>>,
}

impl Flow {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }

    pub fn spawn_producer<P>(
        mut self,
        producer: P,
        tx: Sender<P::Output>,
    ) -> Result<Self, errors::ProcessingError>
    where
        P: Producer + 'static,
    {
        let name =
            self.name.as_ref().map_or_else(|| "flow-prod".to_string(), |n| format!("fprod-{n}"));
        let handler: std::thread::JoinHandle<()> = std::thread::Builder::new()
            .name(name)
            .spawn(move || {
                if let Err(err) = futures::executor::block_on(producer.produce(tx)) {
                    log::error!("Flow producer: {err}");
                }
            })
            .map_err(errors::ProcessingError::Thread)?;

        self.handlers.push(handler);
        Ok(self)
    }

    pub fn spawn_producers<O, E>(
        mut self,
        producers: Vec<Box<dyn RefProducer<Output = O, Error = E>>>,
        tx: Sender<O>,
    ) -> Result<Self, errors::ProcessingError>
    where
        O: Clone + Send + 'static,
        E: std::error::Error + 'static,
    {
        let name =
            self.name.as_ref().map_or_else(|| "flow-prod".to_string(), |n| format!("fprod-{n}"));
        let handler: std::thread::JoinHandle<()> = std::thread::Builder::new()
            .name(name)
            .spawn(move || {
                for producer in producers {
                    if let Err(err) = futures::executor::block_on(producer.produce(tx.clone())) {
                        log::error!("Flow producer: {err}");
                    }
                }
            })
            .map_err(errors::ProcessingError::Thread)?;
        self.handlers.push(handler);
        Ok(self)
    }

    #[allow(clippy::needless_pass_by_value)]
    pub fn spawn_processor<P>(
        mut self,
        processor: P,
        rx: Receiver<P::Input>,
        tx: Sender<P::Output>,
    ) -> Result<Self, errors::ProcessingError>
    where
        P: Processor + 'static,
    {
        self.inner_spawn_processor(processor, rx, tx, 0)?;
        Ok(self)
    }

    #[allow(clippy::needless_pass_by_value)]
    pub fn spawn_processors<P>(
        mut self,
        processor: P,
        rx: Receiver<P::Input>,
        tx: Sender<P::Output>,
    ) -> Result<Self, errors::ProcessingError>
    where
        P: Processor + 'static,
    {
        for i in 0..num_cpus::get() {
            self.inner_spawn_processor(processor.clone(), rx.clone(), tx.clone(), i)?;
        }
        Ok(self)
    }

    pub fn spawn_consumer<C>(
        mut self,
        mut consumer: C,
        rx: Receiver<C::Input>,
    ) -> Result<Self, errors::ProcessingError>
    where
        C: Consumer + 'static,
    {
        let name =
            self.name.as_ref().map_or_else(|| "flow-cons".to_string(), |n| format!("fcons-{n}"));
        let handler: std::thread::JoinHandle<()> = std::thread::Builder::new()
            .name(name)
            .spawn(move || {
                futures::executor::block_on(async {
                    loop {
                        match rx.recv().await {
                            Recv::Value(input) => {
                                if let Err(err) = consumer.consume(input).await {
                                    log::error!("Flow consumer: {err}");
                                }
                            }
                            Recv::Closed => {
                                if let Err(err) = consumer.finish().await {
                                    log::error!("Flow consumer (finish): {err}");
                                }
                                break;
                            }
                        }
                    }
                });
            })
            .map_err(errors::ProcessingError::Thread)?;
        self.handlers.push(handler);
        Ok(self)
    }

    // TODO return vec of errors
    pub fn join(self) {
        for handler in self.handlers {
            if let Err(err) = handler.join() {
                log::error!("Flow join: {err:?}");
            }
        }
    }
}

impl Flow {
    #[allow(clippy::needless_pass_by_value)]
    fn inner_spawn_processor<P>(
        &mut self,
        mut processor: P,
        rx: Receiver<P::Input>,
        tx: Sender<P::Output>,
        i: usize,
    ) -> Result<(), errors::ProcessingError>
    where
        P: Processor + 'static,
    {
        let name = self
            .name
            .as_ref()
            .map_or_else(|| format!("flow-proc-{i}"), |n| format!("fproc-{n}-{i}"));
        let handler: std::thread::JoinHandle<()> = std::thread::Builder::new()
            .name(name)
            .spawn(move || {
                futures::executor::block_on(async {
                    loop {
                        match rx.recv().await {
                            Recv::Value(input) => {
                                if let Err(err) = processor.process(input, tx.clone()).await {
                                    log::error!("Flow processor: {err}");
                                }
                            }
                            Recv::Closed => {
                                if let Err(err) = processor.finish(tx).await {
                                    log::error!("Flow processor (finish): {err}");
                                }
                                break;
                            }
                        }
                    }
                });
            })
            .map_err(errors::ProcessingError::Thread)?;
        self.handlers.push(handler);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    #[allow(dead_code)]
    #[derive(thiserror::Error, Debug)]
    enum TestError {
        #[error("Error")]
        Error,
    }

    #[derive(Clone, Debug)]
    struct Collector {
        pub value: usize,
    }

    impl Collector {
        pub fn new() -> Self {
            Self { value: 0 }
        }

        pub fn merge(&mut self, other: Collector) {
            self.value += other.value;
        }
    }

    #[derive(Clone, Debug)]
    struct TestProducer1 {}

    #[async_trait]
    impl Producer for TestProducer1 {
        type Output = i32;
        type Error = TestError;

        async fn produce(self, tx: Sender<Self::Output>) -> Result<(), Self::Error> {
            tx.send(30).await;
            tx.send(-4).await;
            tx.send(50).await;
            Ok(())
        }
    }

    #[derive(Clone, Debug)]
    struct TestProcessor1 {}

    #[async_trait]
    impl Processor for TestProcessor1 {
        type Input = i32;
        type Output = u32;
        type Error = TestError;

        async fn process(
            &mut self,
            input: Self::Input,
            tx: Sender<Self::Output>,
        ) -> Result<(), Self::Error> {
            if input > 0 {
                tx.send(input as Self::Output).await;
            }
            Ok(())
        }

        async fn finish(self, _tx: Sender<Self::Output>) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[derive(Clone)]
    struct TestConsumer1 {
        collector: Arc<Mutex<Collector>>,
    }

    impl TestConsumer1 {
        pub fn create() -> (Self, Arc<Mutex<Collector>>) {
            let collector = Arc::new(Mutex::new(Collector::new()));
            (Self { collector: collector.clone() }, collector)
        }
    }

    #[async_trait]
    impl Consumer for TestConsumer1 {
        type Input = u32;
        type Error = TestError;

        async fn consume(&mut self, input: Self::Input) -> Result<(), Self::Error> {
            self.collector.lock().unwrap().value += input as usize;
            Ok(())
        }

        async fn finish(self) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[derive(Clone, Debug)]
    struct TestProducer2 {}

    #[async_trait]
    impl Producer for TestProducer2 {
        type Output = usize;
        type Error = TestError;

        async fn produce(self, tx: Sender<Self::Output>) -> Result<(), Self::Error> {
            for i in 1..10usize {
                tx.send(i).await;
            }
            Ok(())
        }
    }

    #[derive(Clone, Debug)]
    struct TestProcessor2 {
        collector: Collector,
    }

    impl TestProcessor2 {
        pub fn new() -> Self {
            Self { collector: Collector::new() }
        }
    }

    #[async_trait]
    impl Processor for TestProcessor2 {
        type Input = usize;
        type Output = Collector;
        type Error = TestError;

        async fn process(
            &mut self,
            input: Self::Input,
            _tx: Sender<Self::Output>,
        ) -> Result<(), Self::Error> {
            self.collector.value += input;
            Ok(())
        }

        async fn finish(self, tx: Sender<Self::Output>) -> Result<(), Self::Error> {
            tx.send(self.collector).await;
            Ok(())
        }
    }

    #[derive(Clone)]
    struct TestConsumer2 {
        collector: Arc<Mutex<Collector>>,
    }

    impl TestConsumer2 {
        pub fn create() -> (Self, Arc<Mutex<Collector>>) {
            let collector = Arc::new(Mutex::new(Collector::new()));
            (Self { collector: collector.clone() }, collector)
        }
    }

    #[async_trait]
    impl Consumer for TestConsumer2 {
        type Input = Collector;
        type Error = TestError;

        async fn consume(&mut self, input: Self::Input) -> Result<(), Self::Error> {
            self.collector.lock().unwrap().merge(input);
            Ok(())
        }

        async fn finish(self) -> Result<(), Self::Error> {
            self.collector.lock().unwrap().value *= 10;
            Ok(())
        }
    }

    #[test]
    fn test() {
        let (tx11, rx11) = bounded::<i32>();
        let (tx12, rx12) = bounded::<u32>();

        let producer1 = TestProducer1 {};
        let processor1 = TestProcessor1 {};
        let (consumer1, collector1) = TestConsumer1::create();

        let (tx21, rx21) = bounded::<usize>();
        let (tx22, rx22) = bounded::<Collector>();

        let producer2 = TestProducer2 {};
        let processor2 = TestProcessor2::new();
        let (consumer2, collector2) = TestConsumer2::create();

        Flow::new()
            .name("1")
            .spawn_producer(producer1, tx11)
            .unwrap()
            .spawn_processors(processor1, rx11, tx12)
            .unwrap()
            .spawn_consumer(consumer1, rx12)
            .unwrap()
            .name("2")
            .spawn_producer(producer2, tx21)
            .unwrap()
            .spawn_processors(processor2, rx21, tx22)
            .unwrap()
            .spawn_consumer(consumer2, rx22)
            .unwrap()
            .join();

        assert_eq!(collector1.lock().unwrap().value, 80);
        assert_eq!(collector2.lock().unwrap().value, 450);
    }
}
