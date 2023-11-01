use std::{
    marker::PhantomData,
    task::{Context, Poll},
};

use futures::future::BoxFuture;
use hyper::{service::Service, Request};
use swagger::{Push, XSpanIdString};

use crate::{config::SecretConfig, db::Db};

swagger::new_context_type!(SustainityContext, EmptyContext, swagger::XSpanIdString, Db);

pub struct MakeAddContext<T, A> {
    inner: T,
    config: SecretConfig,
    marker: PhantomData<A>,
}

impl<T, A, B, Z> MakeAddContext<T, A>
where
    A: Default + Push<XSpanIdString, Result = B>,
    B: Push<Db, Result = Z>,
{
    pub fn new(inner: T, config: SecretConfig) -> MakeAddContext<T, A> {
        MakeAddContext { inner, config, marker: PhantomData }
    }
}

impl<Target, T, A, B, Z> Service<Target> for MakeAddContext<T, A>
where
    Target: Send,
    A: Default + Push<XSpanIdString, Result = B> + Send,
    B: Push<Db, Result = Z>,
    Z: Send + 'static,
    T: Service<Target> + Send,
    T::Future: Send + 'static,
{
    type Error = T::Error;
    type Response = AddContext<T::Response, A, B, Z>;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, target: Target) -> Self::Future {
        let service = self.inner.call(target);
        let config = self.config.clone();
        Box::pin(async move { Ok(AddContext::new(service.await?, config)) })
    }
}

pub struct AddContext<T, A, B, Z>
where
    A: Default + Push<XSpanIdString, Result = B>,
    B: Push<Db, Result = Z>,
{
    inner: T,
    config: SecretConfig,
    marker: PhantomData<A>,
}

impl<T, A, B, Z> AddContext<T, A, B, Z>
where
    A: Default + Push<XSpanIdString, Result = B>,
    B: Push<Db, Result = Z>,
{
    pub fn new(inner: T, config: SecretConfig) -> Self {
        AddContext { inner, config, marker: PhantomData }
    }
}

impl<T, A, B, Z, ReqBody> Service<Request<ReqBody>> for AddContext<T, A, B, Z>
where
    A: Default + Push<XSpanIdString, Result = B>,
    B: Push<Db, Result = Z>,
    Z: Send + 'static,
    T: Service<(Request<ReqBody>, Z)>,
{
    type Error = T::Error;
    type Future = T::Future;
    type Response = T::Response;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        log::info!("Request: {} {}", request.method(), request.uri());
        let context = A::default().push(XSpanIdString::get_or_generate(&request));
        let context = context.push(Db::new(self.config.clone()));
        self.inner.call((request, context))
    }
}
