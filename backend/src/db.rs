use std::collections::HashMap;

use arangors::Connection;
use serde::Deserialize;
use serde_json::value::Value;
use snafu::prelude::*;

use sustainity_api::models as api;
use sustainity_models::store::{LibraryItem, Organisation, Presentation, Product};

use crate::{
    config::SecretConfig,
    errors,
    models::{OrganisationSearchResult, ProductSearchResult},
};

const DB_NAME_SUSTAINITY: &str = "sustainity";

macro_rules! db {
    ($db:expr, $config:ident) => {{
        let name = $db.to_string();
        let conn =
            Connection::establish_basic_auth(&$config.host, &$config.user, &$config.password)
                .await
                .context(errors::ConnectSnafu)?;
        conn.db(&name).await.context(errors::DatabaseSnafu { name })?
    }};
}

#[derive(Default)]
pub struct Query {
    db_name: String,
    query: String,
    vars: HashMap<&'static str, Value>,
}

impl Query {
    pub fn builder(db_name: &str) -> Self {
        Self { db_name: db_name.to_string(), query: String::new(), vars: HashMap::new() }
    }

    pub fn get_query(&self) -> String {
        self.query.clone()
    }

    pub fn line(mut self, line: &'static str) -> Self {
        self.query.push_str(line);
        self.query.push('\n');
        self
    }

    pub fn cond(self, line: &'static str, cond: bool) -> Self {
        if cond {
            self.line(line)
        } else {
            self
        }
    }

    pub fn bind(mut self, from: &'static str, to: impl Into<Value>) -> Self {
        self.vars.insert(from, to.into());
        self
    }

    pub fn bond(self, from: &'static str, to: Option<impl Into<Value>>) -> Self {
        if let Some(to) = to {
            self.bind(from, to)
        } else {
            self
        }
    }

    pub fn build(&self) -> arangors::AqlQuery {
        arangors::AqlQuery::builder().query(&self.query).bind_vars(self.vars.clone()).build()
    }

    pub async fn all<R>(self, config: &SecretConfig) -> Result<Vec<R>, errors::DbError>
    where
        for<'de> R: Clone + std::fmt::Debug + Deserialize<'de>,
    {
        let db = db!(self.db_name, config);
        let results: Vec<R> = db
            .aql_query(self.build())
            .await
            .context(errors::QuerySnafu { query: self.get_query() })?;

        Ok(results)
    }

    pub async fn one<R>(self, config: &SecretConfig) -> Result<Option<R>, errors::DbError>
    where
        for<'de> R: Deserialize<'de> + Clone,
        R: std::fmt::Debug,
    {
        let db = db!(self.db_name, config);
        let results: Vec<R> = db
            .aql_query(self.build())
            .await
            .context(errors::QuerySnafu { query: self.get_query() })?;
        Ok(results.first().cloned())
    }
}

#[derive(Clone)]
pub struct Db {
    config: SecretConfig,
}

impl Db {
    pub fn new(config: SecretConfig) -> Self {
        Self { config }
    }

    pub async fn get_library_contents(&self) -> Result<Vec<LibraryItem>, errors::DbError> {
        Query::builder(DB_NAME_SUSTAINITY)
            .line("WITH library")
            .line("FOR i IN library")
            .line("    RETURN i")
            .all(&self.config)
            .await
    }

    pub async fn get_library_item(&self, id: &str) -> Result<Option<LibraryItem>, errors::DbError> {
        Query::builder(DB_NAME_SUSTAINITY)
            .line("WITH library")
            .line("FOR i IN library")
            .line("    FILTER i.id == @id")
            .line("    RETURN i")
            .bind("id", id)
            .one(&self.config)
            .await
    }

    pub async fn get_presentation(
        &self,
        id: &str,
    ) -> Result<Option<Presentation>, errors::DbError> {
        Query::builder(DB_NAME_SUSTAINITY)
            .line("WITH presentations")
            .line("FOR p IN presentations")
            .line("    FILTER p.id == @id")
            .line("    RETURN p")
            .bind("id", id)
            .one(&self.config)
            .await
    }

    pub async fn get_organisation(
        &self,
        id_variant: api::OrganisationIdVariant,
        id: &str,
    ) -> Result<Option<Organisation>, errors::DbError> {
        let builder = Query::builder(DB_NAME_SUSTAINITY);
        match id_variant {
            api::OrganisationIdVariant::Wiki => builder
                .line("WITH organisations, organisation_wiki_ids, organisation_wiki_id_edges")
                .line("FOR w IN organisation_wiki_ids")
                .line("    FILTER w._key == @id")
                .line("    FOR o IN OUTBOUND w organisation_wiki_id_edges")
                .line("        RETURN o"),
            api::OrganisationIdVariant::Vat => builder
                .line("WITH organisations, organisation_vat_ids, organisation_vat_id_edges")
                .line("FOR v IN organisation_vat_ids")
                .line("    FILTER v._key == @id")
                .line("    FOR o IN OUTBOUND v organisation_vat_ids_edges")
                .line("        RETURN o"),
            api::OrganisationIdVariant::Www => builder
                .line("WITH organisations, organisation_wwws, organisation_www_edges")
                .line("FOR w IN organisation_wwws")
                .line("    FILTER w._key == @id")
                .line("    FOR o IN OUTBOUND w organisation_wwws_edges")
                .line("        RETURN o"),
        }
        .bind("id", id)
        .one(&self.config)
        .await
    }

    pub async fn find_organisation_products(
        &self,
        id: &str,
    ) -> Result<Vec<Product>, errors::DbError> {
        Query::builder(DB_NAME_SUSTAINITY)
            .line("WITH organisations, products, manufacturing_edges")
            .line("FOR o IN organisations")
            .line("    FILTER o._key == @id")
            .line("    FOR p IN 1..1 OUTBOUND o manufacturing_edges")
            .line("        RETURN p")
            .bind("id", id)
            .all(&self.config)
            .await
    }

    pub async fn get_product(
        &self,
        id_variant: api::ProductIdVariant,
        id: &str,
    ) -> Result<Option<Product>, errors::DbError> {
        let builder = Query::builder(DB_NAME_SUSTAINITY);
        match id_variant {
            api::ProductIdVariant::Ean => builder
                .line("WITH product_eans, product_ean_edges, products")
                .line("FOR e IN product_eans")
                .line("    FILTER e._key == @id")
                .line("    FOR p IN OUTBOUND e product_eans_edges")
                .line("        RETURN p"),
            api::ProductIdVariant::Gtin => builder
                .line("WITH product_gtins, product_gtin_edges, products")
                .line("FOR g IN product_gtins")
                .line("    FILTER g._key == @id")
                .line("    FOR p IN OUTBOUND g product_gtin_edges")
                .line("        RETURN p"),
            api::ProductIdVariant::Wiki => builder
                .line("WITH product_wiki_ids, product_wiki_id_edges, products")
                .line("FOR w IN product_wiki_ids")
                .line("    FILTER w._key == @id")
                .line("    FOR p IN OUTBOUND w product_wiki_id_edges")
                .line("        RETURN p"),
        }
        .bind("id", id)
        .one(&self.config)
        .await
    }

    pub async fn find_product_manufacturers(
        &self,
        id: &str,
    ) -> Result<Vec<Organisation>, errors::DbError> {
        Query::builder(DB_NAME_SUSTAINITY)
            .line("WITH organisations, products, manufacturing_edges")
            .line("FOR p IN products")
            .line("    FILTER p._key == @id")
            .line("    FOR o IN INBOUND p manufacturing_edges")
            .line("        RETURN o")
            .bind("id", id)
            .all(&self.config)
            .await
    }

    pub async fn find_product_categories(&self, id: &str) -> Result<Vec<String>, errors::DbError> {
        Query::builder(DB_NAME_SUSTAINITY)
            .line("WITH categories, products, category_edges")
            .line("FOR p IN products")
            .line("    FILTER p._key == @id")
            .line("    FOR c IN 1..1 INBOUND p category_edges")
            .line("        RETURN c._key")
            .bind("id", id)
            .all(&self.config)
            .await
    }

    pub async fn find_product_alternatives(
        &self,
        id: &str,
        category: &str,
        region_code: Option<&str>,
    ) -> Result<Vec<Product>, errors::DbError> {
        let r = region_code.is_some();
        Query::builder(DB_NAME_SUSTAINITY)
            .line("WITH categories, products, category_edges")
            .line("FOR c IN categories")
            .line("    FILTER c._key == @category")
            .line("    FOR p IN 1..1 OUTBOUND c category_edges")
            .line("        FILTER p._key != @id")
            .cond("        FILTER p.regions.variant == \"all\"", r)
            .cond("            OR @region_code IN p.regions.content", r)
            .line("        LET score")
            .line("          = (@id IN p.follows)")
            .line("          + 0.90 * (p.certifications.bcorp != null)")
            .line("          + 0.90 * (p.certifications.eu_ecolabel != null)")
            .line("          + 0.60 * 0.01 * p.certifications.fti.score")
            .line("          + 0.30 * (p.certifications.tco != null)")
            .line("        LET randomized_score = score + 0.01 * RAND()")
            .line("        SORT randomized_score DESC")
            .line("        LIMIT 10")
            .line("        RETURN p")
            .bind("id", id)
            .bind("category", category)
            .bond("region_code", region_code)
            .all(&self.config)
            .await
    }

    pub async fn search_organisations_exact_by_keyword(
        &self,
        matching: &str,
    ) -> Result<Vec<OrganisationSearchResult>, errors::DbError> {
        Query::builder(DB_NAME_SUSTAINITY)
            .line("FOR k IN organisation_keywords")
            .line("    FILTER k.keyword == @match")
            .line("    FOR o IN 1..1 OUTBOUND k organisation_keyword_edges")
            .line("        RETURN { id: o._key, ids: o.ids, name: o.names[0] }")
            .bind("match", matching)
            .all(&self.config)
            .await
    }

    pub async fn search_organisations_substring_by_website(
        &self,
        matching: &str,
    ) -> Result<Vec<OrganisationSearchResult>, errors::DbError> {
        Query::builder(DB_NAME_SUSTAINITY)
            .line("WITH organisations")
            .line("FOR o IN organisations")
            .line("    FILTER o.websites[? 1")
            .line("        FILTER CONTAINS(CURRENT, @match)")
            .line("      ]")
            .line("    RETURN { id: o._key, ids: o.ids, name: o.names[0] }")
            .bind("match", matching)
            .all(&self.config)
            .await
    }

    pub async fn search_organisations_substring_by_vat_number(
        &self,
        matching: &str,
    ) -> Result<Vec<OrganisationSearchResult>, errors::DbError> {
        Query::builder(DB_NAME_SUSTAINITY)
            .line("WITH organisations")
            .line("FOR o IN organisations")
            .line("    FILTER o.vat_numbers[? 1")
            .line("        FILTER CONTAINS(CURRENT, @match)")
            .line("      ]")
            .line("    RETURN { id: o._key, ids: o.ids, name: o.names[0] }")
            .bind("match", matching)
            .all(&self.config)
            .await
    }

    pub async fn search_products_exact_by_keyword(
        &self,
        matching: &str,
    ) -> Result<Vec<ProductSearchResult>, errors::DbError> {
        Query::builder(DB_NAME_SUSTAINITY)
            .line("WITH products, product_keywords, product_keyword_edges")
            .line("FOR k IN product_keywords")
            .line("    FILTER k.keyword == @match")
            .line("    FOR p IN 1..1 OUTBOUND k product_keyword_edges")
            .line("        RETURN { id: p._key, ids: p.ids, name: p.names[0] }")
            .bind("match", matching)
            .all(&self.config)
            .await
    }

    pub async fn search_products_exact_by_gtin(
        &self,
        matching: &str,
    ) -> Result<Vec<ProductSearchResult>, errors::DbError> {
        Query::builder(DB_NAME_SUSTAINITY)
            .line("WITH products, product_gtins, product_gtin_edges")
            .line("FOR g IN product_gtins")
            .line("    FILTER g._key == @match")
            .line("    FOR p IN OUTBOUND g product_gtin_edges")
            .line("        RETURN { id: p._key, ids: p.ids, name: p.names[0] }")
            .bind("match", matching)
            .all(&self.config)
            .await
    }
}
