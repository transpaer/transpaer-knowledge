use std::marker::PhantomData;

use async_trait::async_trait;
use swagger::ApiError;

use sustainity_api::{
    models::{LibraryContents, LibraryTopic},
    Api, CheckHealthResponse, GetAlternativesResponse, GetLibraryItemResponse, GetLibraryResponse,
    GetOrganisationResponse, GetProductResponse, SearchByTextResponse,
};

use crate::{db::Db, retrieve};

const CORS_ORIGIN: &str = "*";
const CORS_METHODS: &str = "GET, POST, DELETE, OPTIONS";
const CORS_HEADERS: &str = "Origin, Content-Type";

fn get<T, C: swagger::Has<T>>(context: &C) -> &T {
    <C as swagger::Has<T>>::get(context)
}

#[derive(Clone)]
pub struct Server<C> {
    marker: PhantomData<C>,
}

impl<C> Server<C> {
    pub fn new() -> Self {
        Server { marker: PhantomData }
    }
}

#[async_trait]
impl<C> Api<C> for Server<C>
where
    C: swagger::Has<swagger::XSpanIdString> + swagger::Has<Db> + Send + Sync,
{
    async fn check_health(&self, _context: &C) -> Result<CheckHealthResponse, ApiError> {
        Ok(CheckHealthResponse::Ok {
            access_control_allow_origin: CORS_ORIGIN.to_string(),
            access_control_allow_methods: CORS_METHODS.to_string(),
            access_control_allow_headers: CORS_HEADERS.to_string(),
        })
    }

    async fn get_library(&self, context: &C) -> Result<GetLibraryResponse, ApiError> {
        let db = get::<Db, C>(context);
        let items = retrieve::library_contents(db).await?;
        Ok(GetLibraryResponse::Ok {
            body: LibraryContents { items },
            access_control_allow_origin: CORS_ORIGIN.to_string(),
            access_control_allow_methods: CORS_METHODS.to_string(),
            access_control_allow_headers: CORS_HEADERS.to_string(),
        })
    }

    async fn get_library_item(
        &self,
        topic: LibraryTopic,
        context: &C,
    ) -> Result<GetLibraryItemResponse, ApiError> {
        let db = get::<Db, C>(context);
        if let Some(item) = retrieve::library_item(topic, db).await? {
            Ok(GetLibraryItemResponse::Ok {
                body: item,
                access_control_allow_origin: CORS_ORIGIN.to_string(),
                access_control_allow_methods: CORS_METHODS.to_string(),
                access_control_allow_headers: CORS_HEADERS.to_string(),
            })
        } else {
            Ok(GetLibraryItemResponse::NotFound {
                access_control_allow_origin: CORS_ORIGIN.to_string(),
                access_control_allow_methods: CORS_METHODS.to_string(),
                access_control_allow_headers: CORS_HEADERS.to_string(),
            })
        }
    }

    async fn search_by_text(
        &self,
        query: String,
        context: &C,
    ) -> Result<SearchByTextResponse, ApiError> {
        let db = get::<Db, C>(context);
        let results = retrieve::search_by_text(query, db).await?;
        Ok(SearchByTextResponse::Ok {
            body: sustainity_api::models::TextSearchResults { results },
            access_control_allow_origin: CORS_ORIGIN.to_string(),
            access_control_allow_methods: CORS_METHODS.to_string(),
            access_control_allow_headers: CORS_HEADERS.to_string(),
        })
    }

    async fn get_organisation(
        &self,
        id: String,
        context: &C,
    ) -> Result<GetOrganisationResponse, ApiError> {
        let db = get::<Db, C>(context);
        if let Some(org) = retrieve::organisation(&id, db).await? {
            Ok(GetOrganisationResponse::Ok {
                body: org,
                access_control_allow_origin: CORS_ORIGIN.to_string(),
                access_control_allow_methods: CORS_METHODS.to_string(),
                access_control_allow_headers: CORS_HEADERS.to_string(),
            })
        } else {
            Ok(GetOrganisationResponse::NotFound {
                access_control_allow_origin: CORS_ORIGIN.to_string(),
                access_control_allow_methods: CORS_METHODS.to_string(),
                access_control_allow_headers: CORS_HEADERS.to_string(),
            })
        }
    }

    async fn get_product(
        &self,
        id: String,
        region: Option<String>,
        context: &C,
    ) -> Result<GetProductResponse, ApiError> {
        let db = get::<Db, C>(context);
        if let Some(prod) = retrieve::product(&id, region.as_deref(), db).await? {
            Ok(GetProductResponse::Ok {
                body: prod,
                access_control_allow_origin: CORS_ORIGIN.to_string(),
                access_control_allow_methods: CORS_METHODS.to_string(),
                access_control_allow_headers: CORS_HEADERS.to_string(),
            })
        } else {
            Ok(GetProductResponse::NotFound {
                access_control_allow_origin: CORS_ORIGIN.to_string(),
                access_control_allow_methods: CORS_METHODS.to_string(),
                access_control_allow_headers: CORS_HEADERS.to_string(),
            })
        }
    }

    async fn get_alternatives(
        &self,
        id: String,
        region: Option<String>,
        context: &C,
    ) -> Result<GetAlternativesResponse, ApiError> {
        let db = get::<Db, C>(context);
        let alternatives = retrieve::product_alternatives(&id, region.as_deref(), db).await?;
        Ok(GetAlternativesResponse::Ok {
            body: alternatives,
            access_control_allow_origin: CORS_ORIGIN.to_string(),
            access_control_allow_methods: CORS_METHODS.to_string(),
            access_control_allow_headers: CORS_HEADERS.to_string(),
        })
    }
}
