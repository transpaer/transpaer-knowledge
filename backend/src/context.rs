use std::{
    marker::PhantomData,
    task::{Context, Poll},
};

use futures::future::BoxFuture;
use hyper::{service::Service, Request};
use swagger::{Push, XSpanIdString};

swagger::new_context_type!(SustainityContext, EmptyContext, swagger::XSpanIdString);

pub struct MakeAddContext<T, A> {
    inner: T,
    marker: PhantomData<A>,
}

impl<T, A, Z> MakeAddContext<T, A>
where
    A: Default + Push<XSpanIdString, Result = Z>,
{
    pub fn new(inner: T) -> MakeAddContext<T, A> {
        MakeAddContext { inner, marker: PhantomData }
    }
}

impl<Target, T, A, Z> Service<Target> for MakeAddContext<T, A>
where
    Target: Send,
    A: Default + Push<XSpanIdString, Result = Z> + Send,
    Z: Send + 'static,
    T: Service<Target> + Send,
    T::Future: Send + 'static,
{
    type Error = T::Error;
    type Response = AddContext<T::Response, A, Z>;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, target: Target) -> Self::Future {
        let service = self.inner.call(target);
        Box::pin(async move { Ok(AddContext::new(service.await?)) })
    }
}

pub struct AddContext<T, A, Z>
where
    A: Default + Push<XSpanIdString, Result = Z>,
{
    inner: T,
    marker: PhantomData<A>,
}

impl<T, A, Z> AddContext<T, A, Z>
where
    A: Default + Push<XSpanIdString, Result = Z>,
{
    pub fn new(inner: T) -> Self {
        AddContext { inner, marker: PhantomData }
    }
}

impl<T, A, Z, ReqBody> Service<Request<ReqBody>> for AddContext<T, A, Z>
where
    A: Default + Push<XSpanIdString, Result = Z>,
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
        self.inner.call((request, context))
    }
}
