use std::collections::HashMap;

use rand::Rng;
use snafu::prelude::*;

use transpaer_api::models as api;
use transpaer_models::{
    buckets::{AppStore, DbStore},
    ids, store, utils,
};

use crate::{
    errors::{self, BackendError},
    models::{OrganisationSearchResult, ProductSearchResult, SearchResultId},
};

const CATEGORY_DBID_SEPARATOR: char = '/';
const CATEGORY_PARAM_SEPARATOR: char = '.';

#[derive(Clone, Debug, PartialEq)]
struct ScoredResult {
    score: f64,
    result: api::TextSearchResult,
}

impl ScoredResult {
    pub fn with_added_score(&mut self, score: f64) {
        self.score += score;
    }
}

#[derive(Clone, Debug, Default)]
struct ResultCollector {
    results: HashMap<SearchResultId, ScoredResult>,
}

impl ResultCollector {
    // Adds results by giving them some score.
    //
    // The score is better if:
    // - the matched keyword is closer to the beginning of the query
    // - the matched keyword constitutes the longer part of the whole label
    pub fn add(
        &mut self,
        results: &[(SearchResultId, api::TextSearchResult)],
        matching: &str,
        index: Option<usize>,
    ) {
        let index_score = if let Some(index) = index { 1.0 / (index + 1) as f64 } else { 10.0 };

        for (id, result) in results {
            let item_score = matching.len() as f64 / result.label.len() as f64;
            let total_score = 1.0 + index_score + item_score;

            self.results
                .entry(id.clone())
                .and_modify(|e| e.with_added_score(total_score))
                .or_insert_with(|| ScoredResult { score: total_score, result: result.clone() });
        }
    }

    pub fn add_organisations(
        &mut self,
        results: Vec<OrganisationSearchResult>,
        matching: &str,
        index: Option<usize>,
    ) {
        let results: Vec<(SearchResultId, api::TextSearchResult)> =
            results.into_iter().filter_map(|r| r.convert()).collect();
        self.add(&results, matching, index)
    }

    pub fn add_products(
        &mut self,
        results: Vec<ProductSearchResult>,
        matching: &str,
        index: Option<usize>,
    ) {
        let results: Vec<(SearchResultId, api::TextSearchResult)> =
            results.into_iter().filter_map(|r| r.convert()).collect();
        self.add(&results, matching, index)
    }

    pub fn gather_scored_results(self) -> Vec<ScoredResult> {
        use std::cmp::Ordering;

        let mut results: Vec<ScoredResult> = self.results.into_values().collect();
        results.sort_by(|a, b| match PartialOrd::partial_cmp(&b.score, &a.score) {
            None | Some(Ordering::Equal) => Ord::cmp(&a.result.label, &b.result.label),
            Some(ordering) => ordering,
        });
        results
    }

    pub fn gather_results(self) -> Vec<api::TextSearchResult> {
        self.gather_scored_results().into_iter().map(|r| r.result).collect()
    }
}

#[derive(Debug, Clone)]
pub struct Retriever {
    db: DbStore,
    app: AppStore,
}

impl Retriever {
    pub fn new(path: &str) -> Result<Self, BackendError> {
        let path = std::path::Path::new(path);
        let db = DbStore::new(&path.join("db"))?;
        let app = AppStore::new(&path.join("app"))?;
        Ok(Self { db, app })
    }

    pub fn library_contents(&self) -> Result<Vec<api::LibraryItemShort>, BackendError> {
        let library = self.app.get_library_bucket()?;
        Ok(library
            .gather()?
            .into_iter()
            .filter_map(|(_topic, item)| item.try_into_api_short().ok())
            .collect())
    }

    pub fn library_item(
        &self,
        topic: api::LibraryTopic,
    ) -> Result<Option<api::LibraryItemFull>, BackendError> {
        let topic_name = topic.to_string();
        let library = self.app.get_library_bucket()?;
        if let Some(item) = library.get(&topic_name)? {
            let presentations = self.app.get_presentation_bucket()?;
            let presentation = presentations.get(&topic_name)?.map(|p| p.into_api());
            let item = item.try_into_api_full(presentation)?;
            Ok(Some(item))
        } else {
            Ok(None)
        }
    }

    pub fn organisation(
        &self,
        id_variant: api::OrganisationIdVariant,
        id: &str,
    ) -> Result<Option<api::OrganisationFull>, BackendError> {
        if let Some(organisation_id) = self.organisation_id(id_variant, id)? {
            let orgs = self.db.get_organisation_bucket()?;
            if let Some(org) = orgs.get(&organisation_id)? {
                let products = self.short_products(&org.products)?;
                let org = org.into_api_full(products);
                Ok(Some(org))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub fn product(
        &self,
        id_variant: api::ProductIdVariant,
        id: &str,
        region: Option<&str>,
    ) -> Result<Option<api::ProductFull>, BackendError> {
        if let Some(product_id) = self.product_id(id_variant, id)? {
            let prods = self.db.get_product_bucket()?;
            if let Some(prod) = prods.get(&product_id)? {
                let manufacturers = self.short_organisations(&prod.manufacturers)?;
                let alternatives =
                    self.product_alternatives_impl(product_id, &prod.categories, region)?;
                let prod = prod.into_api_full(manufacturers, alternatives);
                Ok(Some(prod))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub fn product_alternatives(
        &self,
        id_variant: api::ProductIdVariant,
        id: &str,
        region: Option<&str>,
    ) -> Result<Option<Vec<api::CategoryAlternatives>>, BackendError> {
        if let Some(product_id) = self.product_id(id_variant, id)? {
            let prods = self.db.get_product_bucket()?;
            if let Some(prod) = prods.get(&product_id)? {
                let alternatives =
                    self.product_alternatives_impl(product_id, &prod.categories, region)?;
                Ok(Some(alternatives))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub fn category(
        &self,
        category_param: String,
    ) -> Result<Option<api::CategoryFull>, BackendError> {
        let category_name = Self::decode_category_param(&category_param);
        let categories = self.db.get_categories_bucket()?;
        let products = self.db.get_product_bucket()?;

        if let Some(category) = categories.get(&category_name)? {
            let mut results = Vec::new();
            if let Some(products_ids) = &category.products {
                for product_id in products_ids {
                    if let Some(product) = products.get(product_id)? {
                        results.push((product.score(), product));
                    }
                }
            }
            results.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
            results.truncate(100);
            let results = results.iter().map(|r| r.1.clone().into_api_short()).collect();
            let subcategories = Self::prepare_subcategories(&category_param, &category);
            let supercategories = Self::prepare_supercategories(&category_param);

            Ok(Some(api::CategoryFull {
                label: category_name.to_string(),
                products: results,
                status: category.status.into_api(),
                subcategories,
                supercategories,
            }))
        } else {
            log::warn!("Category `{category_name}` not found");
            Ok(None)
        }
    }

    pub fn search_by_text(
        &self,
        query: String,
    ) -> Result<Vec<api::TextSearchResult>, BackendError> {
        let mut collector = ResultCollector::default();
        let mut tokens: Vec<&str> = query.split(' ').collect();
        tokens.retain(|m| !m.is_empty());

        if tokens.len() == 1 {
            let token = tokens.first().unwrap();
            match token.parse::<u64>() {
                Ok(number) => {
                    // Search product by GTIN
                    // TODO: search only if the match can be a valid GTIN
                    if token.len() < 15 {
                        let lowercase_token = token.to_lowercase();
                        let items = self.products_by_token(number)?;
                        collector.add_products(items, &lowercase_token, None);
                    }
                }
                Err(_) => {
                    let items = self.organisations_by_token(token)?;
                    collector.add_organisations(items, token, None);
                }
            }
        }

        // Search organisations and products by keyword
        let keywords: Vec<String> = tokens.into_iter().map(|m| m.to_lowercase()).collect();
        for (i, keyword) in keywords.iter().enumerate() {
            let items = self.organisations_by_keyword(keyword)?;
            collector.add_organisations(items, keyword, Some(i));
        }
        for (i, keyword) in keywords.iter().enumerate() {
            let items = self.products_by_keyword(keyword)?;
            collector.add_products(items, keyword, Some(i));
        }

        Ok(collector.gather_results())
    }
}

impl Retriever {
    fn organisation_id(
        &self,
        id_variant: api::OrganisationIdVariant,
        id: &str,
    ) -> Result<Option<ids::OrganisationId>, BackendError> {
        Ok(match id_variant {
            api::OrganisationIdVariant::Vat => {
                let ids = self.db.get_vat_id_to_organisation_id_bucket()?;
                ids.get(&ids::VatId::try_from(id).context(errors::ParsingInputSnafu {
                    input: id.to_owned(),
                    variant: errors::InputVariant::VatId,
                })?)?
            }
            api::OrganisationIdVariant::Wiki => {
                let ids = self.db.get_wiki_id_to_organisation_id_bucket()?;
                ids.get(&ids::WikiId::try_from(id).context(errors::ParsingInputSnafu {
                    input: id.to_owned(),
                    variant: errors::InputVariant::WikiId,
                })?)?
            }
            api::OrganisationIdVariant::Www => {
                let ids = self.db.get_www_domain_to_organisation_id_bucket()?;
                ids.get(&id.to_owned())?
            }
        })
    }

    fn product_id(
        &self,
        id_variant: api::ProductIdVariant,
        id: &str,
    ) -> Result<Option<ids::ProductId>, BackendError> {
        Ok(match id_variant {
            api::ProductIdVariant::Ean => {
                let ids = self.db.get_ean_to_product_id_bucket()?;
                ids.get(&ids::Ean::try_from(id).context(errors::ParsingInputSnafu {
                    input: id.to_owned(),
                    variant: errors::InputVariant::Ean,
                })?)?
            }
            api::ProductIdVariant::Gtin => {
                let ids = self.db.get_gtin_to_product_id_bucket()?;
                ids.get(&ids::Gtin::try_from(id).context(errors::ParsingInputSnafu {
                    input: id.to_owned(),
                    variant: errors::InputVariant::Gtin,
                })?)?
            }
            api::ProductIdVariant::Wiki => {
                let ids = self.db.get_wiki_id_to_product_id_bucket()?;
                ids.get(&ids::WikiId::try_from(id).context(errors::ParsingInputSnafu {
                    input: id.to_owned(),
                    variant: errors::InputVariant::WikiId,
                })?)?
            }
        })
    }

    fn short_products(
        &self,
        ids: &[ids::ProductId],
    ) -> Result<Vec<api::ProductShort>, BackendError> {
        let products = self.db.get_product_bucket()?;
        let mut result = Vec::new();
        for id in ids {
            if let Some(product) = products.get(id)? {
                result.push(product.into_api_short());
            } else {
                log::warn!("Product `{id}` not found");
            }
        }
        Ok(result)
    }

    fn short_organisations(
        &self,
        ids: &[ids::OrganisationId],
    ) -> Result<Vec<api::OrganisationShort>, BackendError> {
        let organisations = self.db.get_organisation_bucket()?;
        let mut result = Vec::new();
        for id in ids {
            if let Some(organisation) = organisations.get(id)? {
                result.push(organisation.into_api_short());
            } else {
                log::warn!("Organisation `{id}` not found");
            }
        }
        Ok(result)
    }

    fn product_alternatives_impl(
        &self,
        id: ids::ProductId,
        categories: &[String],
        region_code: Option<&str>,
    ) -> Result<Vec<api::CategoryAlternatives>, BackendError> {
        let mut result = Vec::new();
        for category in categories.iter() {
            // TODO: format the category nicely.
            let category_label = category.clone();
            let category_id = Self::encode_category_param(category);

            let excluded = vec![id.clone()];
            if let Some(alternatives) =
                self.product_category_alternatives(category, region_code, &excluded)?
            {
                result.push(api::CategoryAlternatives {
                    category_id,
                    category_label,
                    alternatives,
                });
            }
        }
        Ok(result)
    }

    fn product_category_alternatives(
        &self,
        category: &String,
        region_code: Option<&str>,
        excluded: &[ids::ProductId],
    ) -> Result<Option<Vec<api::ProductShort>>, BackendError> {
        let categories = self.db.get_categories_bucket()?;
        let products = self.db.get_product_bucket()?;
        if let Some(category) = categories.get(category)? {
            let mut rng = rand::rng();
            // TODO: Do this during precomputation and here only filter by region
            let mut results = Vec::new();
            if let Some(product_ids) = &category.products {
                for product_id in product_ids {
                    if excluded.contains(product_id) {
                        continue;
                    }
                    if let Some(product) = products.get(product_id)? {
                        if product.regions.is_available_in(region_code) {
                            continue;
                        }

                        let score = product.score();
                        let randomized_score = score + rng.random_range(0.0..0.01);
                        results.push((randomized_score, product));
                    }
                }
            }
            results.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
            results.truncate(10);
            Ok(Some(results.iter().map(|r| r.1.clone().into_api_short()).collect()))
        } else {
            log::warn!("Category `{category}` not found");
            Ok(None)
        }
    }

    fn products_by_token(&self, token: u64) -> Result<Vec<ProductSearchResult>, BackendError> {
        let gtins = self.db.get_gtin_to_product_id_bucket()?;
        if let Some(product_id) = gtins.get(&ids::Gtin::new(token))? {
            let products = self.db.get_product_bucket()?;
            if let Some(product) = products.get(&product_id)? {
                Ok(vec![ProductSearchResult::from_db(product_id, product)])
            } else {
                Ok(Vec::new())
            }
        } else {
            Ok(Vec::new())
        }
    }

    fn organisations_by_token(
        &self,
        token: &str,
    ) -> Result<Vec<OrganisationSearchResult>, BackendError> {
        let mut results = Vec::new();
        let lowercase_token = token.to_lowercase();
        let uppercase_token = token.to_uppercase();

        // TODO: prepare index of not full VAT numbers to speedup search.
        // TODO: extract domain from token to speedup search
        let organisations = self.db.get_organisation_bucket()?;
        for (organisation_id, organisation) in organisations.gather()? {
            let mut matched = false;

            for vat in &organisation.ids.vat_ids {
                if vat.as_str().contains(&uppercase_token) {
                    matched = true;
                    break;
                }
            }

            if !matched {
                let domain = utils::extract_domain_from_str(token)
                    .unwrap_or_else(|| lowercase_token.to_owned());
                if organisation.ids.domains.contains(&domain) {
                    matched = true;
                }
            }

            if matched {
                results.push(OrganisationSearchResult::from_db(organisation_id, organisation));
            }
        }
        Ok(results)
    }

    fn products_by_keyword(
        &self,
        keyword: &String,
    ) -> Result<Vec<ProductSearchResult>, BackendError> {
        let mut results = Vec::new();
        let product_keywords = self.db.get_keyword_to_product_ids_bucket()?;
        let products = self.db.get_product_bucket()?;
        if let Some(product_ids) = product_keywords.get(keyword)? {
            for product_id in product_ids {
                if let Some(product) = products.get(&product_id)? {
                    let result = ProductSearchResult::from_db(product_id, product);
                    results.push(result);
                } else {
                    log::warn!("Product `{product_id}` from keyword `{keyword}` not found");
                }
            }
        }
        Ok(results)
    }

    fn organisations_by_keyword(
        &self,
        keyword: &String,
    ) -> Result<Vec<OrganisationSearchResult>, BackendError> {
        let mut results = Vec::new();
        let organisation_keywords = self.db.get_keyword_to_organisation_ids_bucket()?;
        let organisations = self.db.get_organisation_bucket()?;
        if let Some(organisation_ids) = organisation_keywords.get(keyword)? {
            for organisation_id in organisation_ids {
                if let Some(organisation) = organisations.get(&organisation_id)? {
                    let result = OrganisationSearchResult::from_db(organisation_id, organisation);
                    results.push(result);
                } else {
                    log::warn!(
                        "Organisation `{organisation_id}` from keyword `{keyword}` not found"
                    );
                }
            }
        }
        Ok(results)
    }

    fn decode_category_param(param: &str) -> String {
        param.replace(CATEGORY_PARAM_SEPARATOR, &CATEGORY_DBID_SEPARATOR.to_string())
    }

    fn encode_category_param(param: &str) -> String {
        param.replace(CATEGORY_DBID_SEPARATOR, &CATEGORY_PARAM_SEPARATOR.to_string())
    }

    fn prepare_subcategories(
        category_name: &str,
        category: &store::Category,
    ) -> Vec<api::CategoryShort> {
        let sep = CATEGORY_PARAM_SEPARATOR.to_string();
        category
            .subcategories
            .iter()
            .map(|part| {
                let id = if category_name.is_empty() {
                    part.to_string()
                } else {
                    [category_name, part].join(&sep)
                };
                api::CategoryShort { id, label: part.to_string() }
            })
            .collect()
    }

    fn prepare_supercategories(category_name: &str) -> Vec<api::CategoryShort> {
        if category_name.is_empty() {
            return Vec::new();
        }

        let sep = CATEGORY_PARAM_SEPARATOR.to_string();
        let mut supercategories = Vec::new();
        let mut buffer = String::with_capacity(category_name.len());
        for part in category_name.split(&sep) {
            if !buffer.is_empty() {
                buffer += &sep;
            }
            buffer += part;
            supercategories
                .push(api::CategoryShort { id: buffer.clone(), label: part.to_string() });
        }
        supercategories
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    fn prepare_data() -> (
        (SearchResultId, api::TextSearchResult),
        (SearchResultId, api::TextSearchResult),
        (SearchResultId, api::TextSearchResult),
    ) {
        let r1 = (
            SearchResultId::Product("1".to_owned()),
            api::TextSearchResult {
                link: api::TextSearchLinkHack {
                    id: api::Id::from_str("1").unwrap(),
                    product_id_variant: Some(api::ProductIdVariant::Wiki),
                    organisation_id_variant: None,
                },
                label: api::ShortString::from_str("Fairphone 4").unwrap(),
            },
        );

        let r2 = (
            SearchResultId::Product("2".to_owned()),
            api::TextSearchResult {
                link: api::TextSearchLinkHack {
                    id: api::Id::from_str("2").unwrap(),
                    product_id_variant: Some(api::ProductIdVariant::Wiki),
                    organisation_id_variant: None,
                },
                label: api::ShortString::from_str("Samsung 4").unwrap(),
            },
        );

        let r3 = (
            SearchResultId::Product("3".to_owned()),
            api::TextSearchResult {
                link: api::TextSearchLinkHack {
                    id: api::Id::from_str("3").unwrap(),
                    product_id_variant: Some(api::ProductIdVariant::Wiki),
                    organisation_id_variant: None,
                },
                label: api::ShortString::from_str("Fairphone 3").unwrap(),
            },
        );

        (r1, r2, r3)
    }

    /// No sorting hints are given:
    /// - the most repeated item is the first
    /// - ties are proken by sorting by the label
    #[test]
    fn simple() {
        let (r1, r2, r3) = prepare_data();

        let s1 = ScoredResult { result: r1.1.clone(), score: (1.0 + 10.0) + (1.0 + 10.0) };
        let s2 = ScoredResult { result: r3.1.clone(), score: (1.0 + 10.0) };
        let s3 = ScoredResult { result: r2.1.clone(), score: (1.0 + 10.0) };

        let expected_results = [s1, s2, s3];

        {
            let mut collector = ResultCollector::default();
            collector.add(&[r2.clone(), r1.clone()], "", None);
            collector.add(&[r3.clone(), r1.clone()], "", None);

            assert_eq!(collector.gather_scored_results(), expected_results);
        }
        {
            let mut collector = ResultCollector::default();
            collector.add(&[r1.clone(), r3.clone()], "", None);
            collector.add(&[r1.clone(), r2.clone()], "", None);

            assert_eq!(collector.gather_scored_results(), expected_results);
        }
    }

    /// Only position in the query given as a sorting hint.
    /// - the phrase more in the front of the query is given a boost
    #[test]
    fn index() {
        let (r1, r2, r3) = prepare_data();

        let s1 = ScoredResult { result: r1.1.clone(), score: (1.0 + 1.0) + (1.0 + 0.5) };
        let s2 = ScoredResult { result: r2.1.clone(), score: (1.0 + 0.5) };
        let s3 = ScoredResult { result: r3.1.clone(), score: (1.0 + 1.0) };

        let expected_results = [s1, s3, s2];

        let mut collector = ResultCollector::default();
        collector.add(&[r2.clone(), r1.clone()], "", Some(1));
        collector.add(&[r3.clone(), r1.clone()], "", Some(0));

        assert_eq!(collector.gather_scored_results(), expected_results);
    }

    /// Only the matched phrase given as a sorting hint.
    /// - the phrase that constitutes a bigger chunk of the whole label is given a boost
    #[test]
    fn importance() {
        let (r1, r2, r3) = prepare_data();

        let s1 =
            ScoredResult { result: r1.1.clone(), score: (11.0 + 9.0 / 11.0) + (11.0 + 1.0 / 11.0) };
        let s2 = ScoredResult { result: r2.1.clone(), score: (11.0 + 1.0 / 9.0) };
        let s3 = ScoredResult { result: r3.1.clone(), score: (11.0 + 9.0 / 11.0) };

        let expected_results = [s1, s3, s2];

        let mut collector = ResultCollector::default();
        collector.add(&[r2.clone(), r1.clone()], "4", None);
        collector.add(&[r3.clone(), r1.clone()], "Fairphone", None);

        assert_eq!(collector.gather_scored_results(), expected_results);
    }

    /// Tests if the subcategories are prepared correctly in the most common case.
    #[test]
    fn prepare_subcategories() {
        let category = store::Category {
            status: store::CategoryStatus::Incomplete,
            subcategories: vec!["mobile_phones".to_string()],
            products: None,
        };
        let obtained =
            Retriever::prepare_subcategories("electronics.communications.telephony", &category);
        let expected = vec![api::CategoryShort {
            id: "electronics.communications.telephony.mobile_phones".to_owned(),
            label: "mobile_phones".to_owned(),
        }];
        assert_eq!(obtained, expected);
    }

    /// Tests if the subcategories are prepared correctly in case they are prepared for the root category.
    #[test]
    fn prepare_root_subcategories() {
        let category = store::Category {
            status: store::CategoryStatus::Incomplete,
            subcategories: vec!["sub1".to_string(), "sub2".to_string()],
            products: None,
        };
        let obtained = Retriever::prepare_subcategories("", &category);
        let expected = vec![
            api::CategoryShort { id: "sub1".to_owned(), label: "sub1".to_owned() },
            api::CategoryShort { id: "sub2".to_owned(), label: "sub2".to_owned() },
        ];
        assert_eq!(obtained, expected);
    }

    /// Tests if the supercategories are prepared correctly in the most common case.
    #[test]
    fn prepare_supercategories() {
        let obtained = Retriever::prepare_supercategories("electronics.communications.telephony");
        let expected = vec![
            api::CategoryShort { id: "electronics".to_owned(), label: "electronics".to_owned() },
            api::CategoryShort {
                id: "electronics.communications".to_owned(),
                label: "communications".to_owned(),
            },
            api::CategoryShort {
                id: "electronics.communications.telephony".to_owned(),
                label: "telephony".to_owned(),
            },
        ];

        assert_eq!(obtained, expected);
    }

    /// Tests if the supercategories are prepared correctly in case they are prepared for the root category.
    #[test]
    fn prepare_root_supercategories() {
        let obtained = Retriever::prepare_supercategories("");
        let expected = Vec::new();
        assert_eq!(obtained, expected);
    }

    /// Tests if the supercategories are prepared correctly in case they are prepared for a top-level category.
    #[test]
    fn prepare_top_supercategories() {
        let obtained = Retriever::prepare_supercategories("top");
        let expected = vec![api::CategoryShort { id: "top".to_owned(), label: "top".to_owned() }];
        assert_eq!(obtained, expected);
    }
}
