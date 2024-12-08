use std::str::FromStr;

use sustainity_api as api;
use sustainity_models::{ids, store as models};

use swagger::XSpanIdString;

use api::Api;

use crate::{config, errors};

#[derive(Clone, Default)]
struct Context {}

swagger::new_context_type!(SustainityContext, EmptyContext, XSpanIdString);

const TONYS_GTIN: usize = 8_717_677_339_556;
const FAIRPHONE_4_WIKI_ID: usize = 109_851_604;
const FAIRPHONE_ORG_WIKI_ID: usize = 5_019_402;
const BCORP_FAIRPHONE_ID: &str = "001C000001Dz6afIAB";

#[derive(thiserror::Error, Debug)]
enum Finding {
    #[error(" => {complain}\n  -> expected to be true: {expected}")]
    Nok { complain: String, expected: String },

    #[error(" => {complain}\n  -> found:    {found}\n  -> expected: {expected}")]
    Eq { complain: String, found: String, expected: String },

    #[error(" => {0}")]
    Other(String),

    #[error(" => IO: {0}")]
    Io(#[from] std::io::Error),

    #[error(" => API: {0}")]
    Api(#[from] swagger::ApiError),

    #[error(" => API conversion: {0}")]
    ApiConversion(#[from] sustainity_api::models::error::ConversionError),

    #[error(" => API Client: {0}")]
    ApiClient(#[from] sustainity_api::client::ClientInitError),
}

#[derive(Default)]
pub struct Findings {
    findings: Vec<Finding>,
}

impl Findings {
    fn add(&mut self, finding: Finding) {
        self.findings.push(finding);
    }

    fn consider(&mut self, finding: Result<(), Finding>) {
        if let Err(finding) = finding {
            self.findings.push(finding);
        }
    }

    fn report(self) {
        if self.findings.is_empty() {
            log::info!("ALL OK");
        } else {
            for finding in self.findings {
                let string = finding.to_string();
                for line in string.split('\n') {
                    log::error!("{line}");
                }
            }
        }
    }
}

macro_rules! ensure {
    ($expected:expr, $comment:literal) => {
        let expected = &$expected;
        if !expected {
            return Err(Finding::Nok {
                complain: $comment.to_string(),
                expected: format!("{:?}", stringify!($expected)),
            });
        }
    };
}

macro_rules! ensure_eq {
    ($found:expr, $expected:expr, $comment:literal) => {
        let found = &$found;
        let expected = &$expected;
        if found != expected {
            return Err(Finding::Eq {
                complain: $comment.to_string(),
                found: format!("{:?}", found),
                expected: format!("{:?}", expected),
            });
        }
    };
}

pub struct SamplingRunner;

impl SamplingRunner {
    pub async fn run(config: &config::SamplingConfig) -> Result<(), errors::ProcessingError> {
        if let Some(config) = &config.target {
            Self::run_with_json(config);
        }
        if let Some(config) = &config.backend {
            Self::run_with_backend(config).await;
        }
        if config.target.is_none() && config.backend.is_none() {
            log::error!("No data source was given");
        }
        Ok(())
    }

    pub fn run_with_json(config: &config::SamplingTargetConfig) {
        log::info!("Verifying the JSON DB");

        let mut findings = Findings::default();

        findings.consider(Self::check_json_product_ids(config));
        match Self::check_json_product_id_edges(config) {
            Ok(fairphone_4_uid) => {
                findings.consider(Self::check_json_category_edges(&fairphone_4_uid, config));
                match Self::check_json_products(&fairphone_4_uid, config) {
                    Ok(fairphone_org_uid_1) => {
                        findings.consider(Self::check_json_organisation_ids(config));
                        match Self::check_json_organisation_id_edges(config) {
                            Ok(fairphone_org_uid_2) => {
                                findings.consider(Self::check_json_organisations(
                                    &fairphone_org_uid_1,
                                    &fairphone_org_uid_2,
                                    config,
                                ));
                            }
                            Err(finding) => findings.add(finding),
                        }
                    }
                    Err(finding) => findings.add(finding),
                };
            }
            Err(finding) => findings.add(finding),
        }

        findings.report();
    }

    fn check_json_product_ids(config: &config::SamplingTargetConfig) -> Result<(), Finding> {
        log::info!("Iterating product IDs");

        let fairphone_4_wiki_id = FAIRPHONE_4_WIKI_ID.to_string();

        let mut found = false;
        for entry in
            serde_jsonlines::json_lines::<models::IdEntry, _>(&config.product_wiki_ids_path)?
        {
            let entry = entry?;
            if entry.db_key == fairphone_4_wiki_id {
                found = true;
                break;
            }
        }

        if found {
            Ok(())
        } else {
            Err(Finding::Other(format!("Fairphone 4 ID ({fairphone_4_wiki_id}) not found")))
        }
    }

    fn check_json_product_id_edges(
        config: &config::SamplingTargetConfig,
    ) -> Result<String, Finding> {
        log::info!("Iterating product ID edges");

        let fairphone_4_wiki_id = format!("product_wiki_ids/{FAIRPHONE_4_WIKI_ID}");

        let mut found_fairphone_4_uid = None;
        for entry in
            serde_jsonlines::json_lines::<models::Edge, _>(&config.product_wiki_id_edges_path)?
        {
            let entry = entry?;
            if entry.from == fairphone_4_wiki_id {
                found_fairphone_4_uid = entry.to.split('/').nth(1).map(str::to_owned);
                break;
            }
        }

        match found_fairphone_4_uid {
            Some(found_fairphone_4_uid) => Ok(found_fairphone_4_uid),
            None => {
                Err(Finding::Other(format!("Fairphone 4 ({FAIRPHONE_4_WIKI_ID}) edge not found")))
            }
        }
    }

    fn check_json_category_edges(
        fairphone_4_uid: &str,
        config: &config::SamplingTargetConfig,
    ) -> Result<(), Finding> {
        log::info!("Iterating category edges");

        let fairphone_4_uid = format!("products/{fairphone_4_uid}");

        let mut found: usize = 0;
        for entry in serde_jsonlines::json_lines::<models::Edge, _>(&config.category_edges_path)? {
            let entry = entry?;
            if entry.to == fairphone_4_uid {
                found += 1;
            }
        }

        if found == 1 {
            Ok(())
        } else {
            Err(Finding::Other(format!(
                "Fairphone 4 ({FAIRPHONE_4_WIKI_ID}) had wrong numer of categories: {found}"
            )))
        }
    }

    fn check_json_products(
        fairphone_4_uid: &str,
        config: &config::SamplingTargetConfig,
    ) -> Result<String, Finding> {
        let fairphone_4_ids = [FAIRPHONE_4_WIKI_ID.to_string()];

        log::info!("Iterating products");

        let mut found_fairphone_org_uid = None;
        for entry in serde_jsonlines::json_lines::<models::Product, _>(&config.products_path)? {
            let entry = entry?;
            if entry.db_key == fairphone_4_uid {
                ensure_eq!(entry.ids.wiki, fairphone_4_ids, "wrong wiki IDs");
                ensure_eq!(
                    entry.names,
                    vec![models::Text {
                        text: "Fairphone 4".to_owned(),
                        source: models::Source::Wikidata
                    }],
                    "wrong name or source"
                );
                ensure_eq!(
                    entry.certifications,
                    models::Certifications {
                        bcorp: Some(models::BCorpCert { id: BCORP_FAIRPHONE_ID.to_owned() }),
                        eu_ecolabel: None,
                        fti: None,
                        tco: Some(models::TcoCert { brand_name: "FAIRPHONE".to_owned() }),
                    },
                    "wrong certifications"
                );
                ensure_eq!(entry.manufacturer_ids.len(), 1, "wrong number of manufacturers");
                found_fairphone_org_uid = entry.manufacturer_ids.first().cloned();
                break;
            }
        }

        match found_fairphone_org_uid {
            Some(fairphone_org_id) => Ok(fairphone_org_id),
            None => Err(Finding::Other(format!("Fairphone 4 ({FAIRPHONE_4_WIKI_ID}) not found"))),
        }
    }

    fn check_json_organisation_ids(config: &config::SamplingTargetConfig) -> Result<(), Finding> {
        log::info!("Iterating organisation IDs");

        let fairphone_org_wiki_id = FAIRPHONE_ORG_WIKI_ID.to_string();

        let mut found = false;
        for entry in
            serde_jsonlines::json_lines::<models::IdEntry, _>(&config.organisation_wiki_ids_path)?
        {
            let entry = entry?;
            if entry.db_key == fairphone_org_wiki_id {
                found = true;
                break;
            }
        }

        if found {
            Ok(())
        } else {
            Err(Finding::Other(format!(
                "Fairphone organisation ID ({fairphone_org_wiki_id}) not found"
            )))
        }
    }

    fn check_json_organisation_id_edges(
        config: &config::SamplingTargetConfig,
    ) -> Result<String, Finding> {
        log::info!("Iterating organisation ID edges");

        let fairphone_org_wiki_id = format!("organisation_wiki_ids/{FAIRPHONE_ORG_WIKI_ID}");

        let mut found_fairphone_org_uid = None;
        for entry in
            serde_jsonlines::json_lines::<models::Edge, _>(&config.organisation_wiki_id_edges_path)?
        {
            let entry = entry?;
            if entry.from == fairphone_org_wiki_id {
                found_fairphone_org_uid = entry.to.split('/').nth(1).map(str::to_owned);
                break;
            }
        }

        match found_fairphone_org_uid {
            Some(found_fairphone_org_uid) => Ok(found_fairphone_org_uid),
            None => Err(Finding::Other(format!(
                "Fairphone organisation ({FAIRPHONE_ORG_WIKI_ID}) edge not found"
            ))),
        }
    }

    fn check_json_organisations(
        fairphone_org_uid_1: &str,
        fairphone_org_uid_2: &str,
        config: &config::SamplingTargetConfig,
    ) -> Result<(), Finding> {
        let fairphone_org_ids = [FAIRPHONE_ORG_WIKI_ID.to_string()];

        log::info!("Iterating organisations");

        ensure_eq!(
            fairphone_org_uid_1,
            fairphone_org_uid_2,
            "Fairphone organisation IDs were different"
        );

        for entry in
            serde_jsonlines::json_lines::<models::Organisation, _>(&config.organisations_path)?
        {
            let entry = entry?;
            if entry.db_key == fairphone_org_uid_1 {
                ensure_eq!(entry.ids.wiki, fairphone_org_ids, "wrong wiki IDs");
                ensure_eq!(
                    entry.names,
                    vec![
                        models::Text { text: "FAIRPHONE".to_owned(), source: models::Source::Tco },
                        models::Text {
                            text: "Fairphone".to_owned(),
                            source: models::Source::Wikidata,
                        },
                        models::Text {
                            text: "Fairphone".to_owned(),
                            source: models::Source::BCorp,
                        }
                    ],
                    "wrong name or source"
                );
                ensure_eq!(
                    entry.certifications,
                    models::Certifications {
                        bcorp: Some(models::BCorpCert { id: BCORP_FAIRPHONE_ID.to_owned() }),
                        eu_ecolabel: None,
                        fti: None,
                        tco: Some(models::TcoCert { brand_name: "FAIRPHONE".to_owned() }),
                    },
                    "wrong certifications"
                );
            }
        }
        Ok(())
    }

    pub async fn run_with_backend(config: &config::SamplingBackendConfig) {
        log::info!("Verifying the backend");
        let mut findings = Findings::default();

        findings.consider(Self::check_backend_library(config).await);
        findings.consider(Self::check_backend_prod_by_gtin(config).await);
        findings.consider(Self::check_backend_prod_by_wiki_id(config).await);
        findings.consider(Self::check_backend_org_by_wiki_id(config).await);
        findings.consider(Self::check_backend_text_search_by_name(config).await);
        findings.consider(Self::check_backend_text_search_by_gtin(config).await);
        findings.consider(Self::check_backend_text_search_by_www(config).await);

        findings.report();
    }

    async fn check_backend_library(config: &config::SamplingBackendConfig) -> Result<(), Finding> {
        let client = api::Client::try_new_http(&config.url)?;
        let context = SustainityContext::<_, Context>::default();

        let library = client.get_library(&context).await?;
        match library {
            api::GetLibraryResponse::Ok { body: library, .. } => {
                ensure_eq!(library.items.len(), 10, "wrong library length");
            }
        }
        Ok(())
    }

    async fn check_backend_prod_by_gtin(
        config: &config::SamplingBackendConfig,
    ) -> Result<(), Finding> {
        let client = api::Client::try_new_http(&config.url)?;
        let context = SustainityContext::<_, Context>::default();
        let gtin = ids::Gtin::new(TONYS_GTIN);

        let product = client
            .get_product(api::models::ProductIdVariant::Gtin, gtin.to_string(), None, &context)
            .await?;
        match product {
            api::GetProductResponse::Ok { body: product, .. } => {
                ensure_eq!(
                    product.product_ids.gtins,
                    vec![api::models::Id::from_str(&gtin.to_string())?],
                    "wrong GTINs"
                );
                ensure_eq!(
                    product.names,
                    vec![api::models::ShortText {
                        text: api::models::ShortString::from_str("Tony's Chocolonely")?,
                        source: api::models::DataSource::Off
                    }],
                    "wrong name or source"
                );
                ensure_eq!(product.medallions.len(), 1, "wrong number of certifications");
                ensure_eq!(
                    product.medallions[0].variant,
                    api::models::MedallionVariant::Sustainity,
                    "wrong certification"
                );
                ensure!(product.medallions[0].sustainity.is_some(), "wrong certification");
                ensure_eq!(product.manufacturers.len(), 1, "wrong number of manufacturers");
            }
            api::GetProductResponse::NotFound { .. } => {
                return Err(Finding::Other(format!(
                    "Product {:?}:{} not found",
                    api::models::ProductIdVariant::Gtin,
                    TONYS_GTIN,
                )));
            }
        }
        Ok(())
    }

    async fn check_backend_prod_by_wiki_id(
        config: &config::SamplingBackendConfig,
    ) -> Result<(), Finding> {
        let client = api::Client::try_new_http(&config.url)?;
        let context = SustainityContext::<_, Context>::default();

        let product = client
            .get_product(
                api::models::ProductIdVariant::Wiki,
                FAIRPHONE_4_WIKI_ID.to_string(),
                None,
                &context,
            )
            .await?;
        match product {
            api::GetProductResponse::Ok { body: product, .. } => {
                ensure_eq!(
                    product.product_ids.wiki,
                    vec![api::models::Id::from_str(&FAIRPHONE_4_WIKI_ID.to_string())?],
                    "wrong IDs"
                );
                ensure_eq!(
                    product.names,
                    vec![api::models::ShortText {
                        text: api::models::ShortString::from_str("Fairphone 4")?,
                        source: api::models::DataSource::Wiki
                    }],
                    "wrong name or source"
                );
                ensure_eq!(product.medallions.len(), 3, "wrong number of certifications");
                ensure_eq!(
                    product.medallions[0],
                    api::models::Medallion {
                        variant: api::models::MedallionVariant::BCorp,
                        bcorp: Some(api::models::BCorpMedallion {
                            id: api::models::Id::from_str(BCORP_FAIRPHONE_ID)?,
                        }),
                        eu_ecolabel: None,
                        fti: None,
                        sustainity: None,
                        tco: None,
                    },
                    "wrong certification"
                );
                ensure_eq!(
                    product.medallions[1],
                    api::models::Medallion {
                        variant: api::models::MedallionVariant::Tco,
                        bcorp: None,
                        eu_ecolabel: None,
                        fti: None,
                        sustainity: None,
                        tco: Some(api::models::TcoMedallion {
                            brand_name: api::models::ShortString::from_str("FAIRPHONE")?,
                        }),
                    },
                    "wrong certification"
                );
                ensure_eq!(
                    product.medallions[2].variant,
                    api::models::MedallionVariant::Sustainity,
                    "wrong certification"
                );
                ensure!(product.medallions[2].sustainity.is_some(), "wrong certification");
                ensure_eq!(product.alternatives.len(), 1, "wrong number of alternatives");
                ensure_eq!(
                    product.alternatives[0].category,
                    "smartphone",
                    "unwexpected category name"
                );
                ensure_eq!(
                    product.alternatives[0].alternatives.len(),
                    10,
                    "wrong number of alternatives"
                );
                ensure_eq!(product.manufacturers.len(), 1, "wrong number of manufacturers");
            }
            api::GetProductResponse::NotFound { .. } => {
                return Err(Finding::Other(format!(
                    "Product {:?}:{} not found",
                    api::models::ProductIdVariant::Wiki,
                    FAIRPHONE_4_WIKI_ID,
                )));
            }
        }
        Ok(())
    }

    async fn check_backend_org_by_wiki_id(
        config: &config::SamplingBackendConfig,
    ) -> Result<(), Finding> {
        let client = api::Client::try_new_http(&config.url)?;
        let context = SustainityContext::<_, Context>::default();

        let org = client
            .get_organisation(
                api::models::OrganisationIdVariant::Wiki,
                FAIRPHONE_ORG_WIKI_ID.to_string(),
                &context,
            )
            .await?;
        match org {
            api::GetOrganisationResponse::Ok { body: org, .. } => {
                ensure_eq!(
                    org.organisation_ids.wiki,
                    vec![api::models::Id::from_str(&FAIRPHONE_ORG_WIKI_ID.to_string())?],
                    "wrong IDS"
                );
                ensure_eq!(
                    org.names,
                    vec![
                        api::models::ShortText {
                            text: api::models::ShortString::from_str("FAIRPHONE")?,
                            source: api::models::DataSource::Tco
                        },
                        api::models::ShortText {
                            text: api::models::ShortString::from_str("Fairphone")?,
                            source: api::models::DataSource::Wiki
                        },
                        api::models::ShortText {
                            text: api::models::ShortString::from_str("Fairphone")?,
                            source: api::models::DataSource::BCorp
                        },
                    ],
                    "wrong name or source"
                );
                ensure_eq!(
                    org.medallions,
                    vec![
                        api::models::Medallion {
                            variant: api::models::MedallionVariant::BCorp,
                            bcorp: Some(api::models::BCorpMedallion {
                                id: api::models::Id::from_str(BCORP_FAIRPHONE_ID)?
                            }),
                            eu_ecolabel: None,
                            fti: None,
                            sustainity: None,
                            tco: None,
                        },
                        api::models::Medallion {
                            variant: api::models::MedallionVariant::Tco,
                            bcorp: None,
                            eu_ecolabel: None,
                            fti: None,
                            sustainity: None,
                            tco: Some(api::models::TcoMedallion {
                                brand_name: api::models::ShortString::from_str("FAIRPHONE")?,
                            }),
                        },
                    ],
                    "wrong certifications"
                );
            }
            api::GetOrganisationResponse::NotFound { .. } => {
                return Err(Finding::Other(format!(
                    "Organisation {:?}:{} not found",
                    api::models::OrganisationIdVariant::Wiki,
                    FAIRPHONE_ORG_WIKI_ID,
                )));
            }
        }

        Ok(())
    }

    async fn check_backend_text_search_by_name(
        config: &config::SamplingBackendConfig,
    ) -> Result<(), Finding> {
        let client = api::Client::try_new_http(&config.url)?;
        let context = SustainityContext::<_, Context>::default();

        let result = client.search_by_text("fairphone".to_string(), &context).await?;
        match result {
            api::SearchByTextResponse::Ok { body: org, .. } => {
                ensure_eq!(org.results.len(), 7, "looking for fairphone");
                ensure!(
                    org.results
                        .iter()
                        .any(|r| r.link.product_id_variant.is_some() && *r.label == "Fairphone 1"),
                    "not found"
                );
                ensure!(
                    org.results
                        .iter()
                        .any(|r| r.link.product_id_variant.is_some() && *r.label == "Fairphone 2"),
                    "not found"
                );
                ensure!(
                    org.results
                        .iter()
                        .any(|r| r.link.product_id_variant.is_some() && *r.label == "Fairphone 3"),
                    "not found"
                );
                ensure!(
                    org.results
                        .iter()
                        .any(|r| r.link.product_id_variant.is_some() && *r.label == "Fairphone 4"),
                    "not found"
                );
                ensure!(
                    org.results
                        .iter()
                        .any(|r| r.link.product_id_variant.is_some() && *r.label == "Fairphone 5"),
                    "not found"
                );
                ensure!(
                    org.results.iter().any(|r| r.link.product_id_variant.is_some()
                        && *r.label == "Fairphone smartphone"),
                    "not found"
                );
                ensure!(
                    org.results.iter().any(
                        |r| r.link.organisation_id_variant.is_some() && *r.label == "FAIRPHONE"
                    ),
                    "not found"
                );
            }
        }

        Ok(())
    }

    async fn check_backend_text_search_by_gtin(
        config: &config::SamplingBackendConfig,
    ) -> Result<(), Finding> {
        let client = api::Client::try_new_http(&config.url)?;
        let context = SustainityContext::<_, Context>::default();

        let result = client.search_by_text("8717677339556".to_string(), &context).await?;
        match result {
            api::SearchByTextResponse::Ok { body: org, .. } => {
                ensure_eq!(org.results.len(), 1, "looking for tony's");
                ensure!(
                    org.results
                        .iter()
                        .any(|r| r.link.product_id_variant.is_some()
                            && *r.label == "Tony's Chocolonely"),
                    "Tony's not found"
                );
            }
        }

        Ok(())
    }

    async fn check_backend_text_search_by_www(
        config: &config::SamplingBackendConfig,
    ) -> Result<(), Finding> {
        let client = api::Client::try_new_http(&config.url)?;
        let context = SustainityContext::<_, Context>::default();

        let result = client.search_by_text("shein.com".to_string(), &context).await?;
        match result {
            api::SearchByTextResponse::Ok { body: org, .. } => {
                ensure_eq!(org.results.len(), 1, "looking for shein.com");
                ensure!(
                    org.results
                        .iter()
                        .any(|r| r.link.organisation_id_variant.is_some() && *r.label == "SHEIN"),
                    "shein.com not found"
                );
            }
        }

        Ok(())
    }
}
