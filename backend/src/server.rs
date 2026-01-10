// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::marker::PhantomData;

use async_trait::async_trait;
use swagger::ApiError;

use transpaer_api::{
    Api, CheckHealthResponse, GetAlternativesResponse, GetCategoryResponse, GetLibraryItemResponse,
    GetLibraryResponse, GetOrganisationResponse, GetProductResponse, SearchByTextResponse,
    models::{LibraryContents, OrganisationIdVariant, ProductIdVariant, TextSearchResults},
};

use crate::retrieve;

const CORS_ORIGIN: &str = "*";
const CORS_METHODS: &str = "GET, POST, DELETE, OPTIONS";
const CORS_HEADERS: &str = "Origin, Content-Type";

#[derive(Clone)]
pub struct Server<C> {
    retriever: retrieve::Retriever,
    marker: PhantomData<C>,
}

impl<C> Server<C> {
    pub fn new(retriever: retrieve::Retriever) -> Self {
        Server { retriever, marker: PhantomData }
    }
}

#[async_trait]
impl<C> Api<C> for Server<C>
where
    C: swagger::Has<swagger::XSpanIdString> + Send + Sync,
{
    async fn check_health(&self, _context: &C) -> Result<CheckHealthResponse, ApiError> {
        tracing::info_span!("request", request = "health-check");
        Ok(CheckHealthResponse::Ok {
            access_control_allow_origin: CORS_ORIGIN.to_string(),
            access_control_allow_methods: CORS_METHODS.to_string(),
            access_control_allow_headers: CORS_HEADERS.to_string(),
        })
    }

    async fn get_library(&self, _context: &C) -> Result<GetLibraryResponse, ApiError> {
        tracing::info_span!("request", request = "get-library");
        let items = self.retriever.library_contents()?;
        Ok(GetLibraryResponse::Ok {
            body: LibraryContents { items },
            access_control_allow_origin: CORS_ORIGIN.to_string(),
            access_control_allow_methods: CORS_METHODS.to_string(),
            access_control_allow_headers: CORS_HEADERS.to_string(),
        })
    }

    async fn get_library_item(
        &self,
        topic: String,
        _context: &C,
    ) -> Result<GetLibraryItemResponse, ApiError> {
        tracing::info_span!("request", request = "get-library-item", topic);
        if let Some(item) = self.retriever.library_item(&topic)? {
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
        _context: &C,
    ) -> Result<SearchByTextResponse, ApiError> {
        tracing::info_span!("request", request = "search-by-text", query);
        let results = self.retriever.search_by_text(query)?;
        Ok(SearchByTextResponse::Ok {
            body: TextSearchResults { results },
            access_control_allow_origin: CORS_ORIGIN.to_string(),
            access_control_allow_methods: CORS_METHODS.to_string(),
            access_control_allow_headers: CORS_HEADERS.to_string(),
        })
    }

    async fn get_organisation(
        &self,
        id_variant: OrganisationIdVariant,
        id: String,
        _context: &C,
    ) -> Result<GetOrganisationResponse, ApiError> {
        tracing::info_span!("request", request = "get-organisation", %id_variant, organisation_id = %id);
        if let Some(org) = self.retriever.organisation(id_variant, &id)? {
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
        id_variant: ProductIdVariant,
        id: String,
        region: Option<String>,
        _context: &C,
    ) -> Result<GetProductResponse, ApiError> {
        tracing::info_span!("request", request = "get-product", %id_variant, product_id = %id);
        if let Some(prod) = self.retriever.product(id_variant, &id, region.as_deref())? {
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
        id_variant: ProductIdVariant,
        id: String,
        region: Option<String>,
        _context: &C,
    ) -> Result<GetAlternativesResponse, ApiError> {
        tracing::info_span!("request", request = "get-alternatives", %id_variant, product_id = %id, region);
        let alternatives =
            self.retriever.product_alternatives(id_variant, &id, region.as_deref())?;
        Ok(GetAlternativesResponse::Ok {
            body: alternatives.unwrap_or_else(Vec::new),
            access_control_allow_origin: CORS_ORIGIN.to_string(),
            access_control_allow_methods: CORS_METHODS.to_string(),
            access_control_allow_headers: CORS_HEADERS.to_string(),
        })
    }

    async fn get_category(
        &self,
        category_id: String,
        _context: &C,
    ) -> Result<GetCategoryResponse, ApiError> {
        tracing::info_span!("request", request = "get-category", category = %category_id);
        if let Some(category) = self.retriever.category(category_id)? {
            Ok(GetCategoryResponse::Ok {
                body: category,
                access_control_allow_origin: CORS_ORIGIN.to_string(),
                access_control_allow_methods: CORS_METHODS.to_string(),
                access_control_allow_headers: CORS_HEADERS.to_string(),
            })
        } else {
            Ok(GetCategoryResponse::NotFound {
                access_control_allow_origin: CORS_ORIGIN.to_string(),
                access_control_allow_methods: CORS_METHODS.to_string(),
                access_control_allow_headers: CORS_HEADERS.to_string(),
            })
        }
    }
}
