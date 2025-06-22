use std::str::FromStr;

use sustainity_api as api;
use sustainity_models::{
    buckets::{BucketError, DbStore},
    ids, store as models,
};

use swagger::XSpanIdString;

use api::Api;

use crate::{config, errors};

#[derive(Clone, Default)]
struct Context {}

swagger::new_context_type!(SustainityContext, EmptyContext, XSpanIdString);

const TONYS_GTIN: ids::Gtin = ids::Gtin::new(8_717_677_339_556);
const FAIRPHONE_4_WIKI_ID: ids::WikiId = ids::WikiId::new(109_851_604);
const FAIRPHONE_ORG_WIKI_ID: ids::WikiId = ids::WikiId::new(5_019_402);
const BCORP_FAIRPHONE_ID: &str = "001C000001Dz6afIAB";
const BCORP_FAIRPHONE_URL: &str =
    "https://www.bcorporation.net/en-us/find-a-b-corp/company/fairphone/";
const AVENTON_DOMAIN: &str = "aventon.com";
const MELIORA_DOMAIN: &str = "meliorameansbetter.com";

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

    #[error(" => Bucket: {0}")]
    Bucket(#[from] BucketError),

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
            Self::run_with_store(config);
        }
        if let Some(config) = &config.backend {
            Self::run_with_backend(config).await;
        }
        if config.target.is_none() && config.backend.is_none() {
            log::error!("No data source was given");
        }
        Ok(())
    }

    pub fn run_with_store(config: &config::SamplingTargetConfig) {
        log::info!("Verifying the kv store");

        let mut findings = Findings::default();

        findings.consider(Self::check_store_prod_by_wiki_id(config));
        findings.consider(Self::check_store_org_by_wiki_id(config));

        findings.report();
    }

    fn check_store_prod_by_wiki_id(config: &config::SamplingTargetConfig) -> Result<(), Finding> {
        let fairphone_4_ids = [FAIRPHONE_4_WIKI_ID];

        let store = DbStore::new(&config.db_storage)?;
        let product_wiki_ids = store.get_wiki_id_to_product_id_bucket()?;
        let products = store.get_product_bucket()?;

        let unique_id = product_wiki_ids
            .get(&FAIRPHONE_4_WIKI_ID)?
            .ok_or(Finding::Other(format!("Product {FAIRPHONE_4_WIKI_ID:?} not found")))?;
        let entry = products
            .get(&unique_id)?
            .ok_or(Finding::Other(format!("Product with unique ID `{unique_id}` not found")))?;

        ensure_eq!(entry.ids.wiki, fairphone_4_ids, "wrong wiki IDs");
        ensure_eq!(
            entry.names,
            vec![models::Text { text: "Fairphone 4".to_owned(), source: models::Source::Wikidata }],
            "wrong name or source"
        );
        ensure_eq!(
            entry.certifications,
            models::Certifications {
                bcorp: Some(models::BCorpCert {
                    id: BCORP_FAIRPHONE_ID.to_owned(),
                    report_url: BCORP_FAIRPHONE_URL.to_owned(),
                }),
                eu_ecolabel: None,
                fti: None,
                tco: Some(models::TcoCert { brand_name: "FAIRPHONE".to_owned() }),
            },
            "wrong certifications"
        );
        ensure_eq!(entry.manufacturers.len(), 1, "wrong number of manufacturers");
        Ok(())
    }

    fn check_store_org_by_wiki_id(config: &config::SamplingTargetConfig) -> Result<(), Finding> {
        let fairphone_org_ids = [FAIRPHONE_ORG_WIKI_ID];

        let store = DbStore::new(&config.db_storage)?;
        let organisation_wiki_ids = store.get_wiki_id_to_organisation_id_bucket()?;
        let organisations = store.get_organisation_bucket()?;

        let unique_id = organisation_wiki_ids
            .get(&FAIRPHONE_ORG_WIKI_ID)?
            .ok_or(Finding::Other(format!("Organisation {FAIRPHONE_ORG_WIKI_ID:?} not found")))?;
        let entry = organisations.get(&unique_id)?.ok_or(Finding::Other(format!(
            "Oranisation with unique ID `{unique_id:?}` not found"
        )))?;

        ensure_eq!(entry.ids.wiki, fairphone_org_ids, "wrong wiki IDs");
        ensure_eq!(
            entry.names,
            vec![
                models::Text { text: "FAIRPHONE".to_owned(), source: models::Source::Tco },
                models::Text { text: "Fairphone".to_owned(), source: models::Source::Wikidata },
                models::Text { text: "Fairphone".to_owned(), source: models::Source::BCorp }
            ],
            "wrong name or source"
        );
        ensure_eq!(
            entry.certifications,
            models::Certifications {
                bcorp: Some(models::BCorpCert {
                    id: BCORP_FAIRPHONE_ID.to_owned(),
                    report_url: BCORP_FAIRPHONE_URL.to_owned(),
                }),
                eu_ecolabel: None,
                fti: None,
                tco: Some(models::TcoCert { brand_name: "FAIRPHONE".to_owned() }),
            },
            "wrong certifications"
        );
        Ok(())
    }

    pub async fn run_with_backend(config: &config::SamplingBackendConfig) {
        log::info!("Verifying the backend");
        let mut findings = Findings::default();

        findings.consider(Self::check_backend_library(config).await);
        findings.consider(Self::check_backend_prod_by_gtin(config).await);
        findings.consider(Self::check_backend_prod_by_wiki_id(config).await);
        findings.consider(Self::check_backend_org_by_wiki_id(config).await);
        findings.consider(Self::check_backend_org_by_domain(config).await);
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

        let item = client.get_library_item(api::models::LibraryTopic::CertFti, &context).await?;
        match item {
            api::GetLibraryItemResponse::Ok { body: item, .. } => {
                if item.presentation.is_none() {
                    return Err(Finding::Other(
                        "Library item CertFti has no presentation".to_owned(),
                    ));
                }
            }
            api::GetLibraryItemResponse::NotFound { .. } => {
                return Err(Finding::Other(format!(
                    "Library item {:?} not found",
                    api::models::LibraryTopic::CertFti,
                )));
            }
        }

        Ok(())
    }

    async fn check_backend_prod_by_gtin(
        config: &config::SamplingBackendConfig,
    ) -> Result<(), Finding> {
        let client = api::Client::try_new_http(&config.url)?;
        let context = SustainityContext::<_, Context>::default();
        let gtin = TONYS_GTIN.to_string();

        let product = client
            .get_product(api::models::ProductIdVariant::Gtin, gtin.clone(), None, &context)
            .await?;
        match product {
            api::GetProductResponse::Ok { body: product, .. } => {
                ensure_eq!(
                    product.product_ids.gtins,
                    vec![api::models::Id::from_str(&gtin)?],
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
                // TODO: Ensure the manufacturer is known and has correct ID.
                ensure_eq!(product.manufacturers.len(), 0, "wrong number of manufacturers");
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
                FAIRPHONE_4_WIKI_ID.to_canonical_string(),
                None,
                &context,
            )
            .await?;
        match product {
            api::GetProductResponse::Ok { body: product, .. } => {
                ensure_eq!(
                    product.product_ids.wiki,
                    vec![api::models::Id::from_str(&FAIRPHONE_4_WIKI_ID.to_canonical_string())?],
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
                            report_url: api::models::LongString::from_str(BCORP_FAIRPHONE_URL)?,
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
                ensure_eq!(product.alternatives.len(), 1, "wrong number of category alternatives");
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
                    "Product {:?}:{:?} not found",
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
                FAIRPHONE_ORG_WIKI_ID.to_canonical_string(),
                &context,
            )
            .await?;
        match org {
            api::GetOrganisationResponse::Ok { body: org, .. } => {
                ensure_eq!(
                    org.organisation_ids.wiki,
                    vec![api::models::Id::from_str(&FAIRPHONE_ORG_WIKI_ID.to_canonical_string())?],
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
                            source: api::models::DataSource::Sustainity
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
                                id: api::models::Id::from_str(BCORP_FAIRPHONE_ID)?,
                                report_url: api::models::LongString::from_str(BCORP_FAIRPHONE_URL)?,
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
                    "Organisation {:?}:{:?} not found",
                    api::models::OrganisationIdVariant::Wiki,
                    FAIRPHONE_ORG_WIKI_ID,
                )));
            }
        }

        Ok(())
    }

    async fn check_backend_org_by_domain(
        config: &config::SamplingBackendConfig,
    ) -> Result<(), Finding> {
        let client = api::Client::try_new_http(&config.url)?;
        let context = SustainityContext::<_, Context>::default();

        let org = client
            .get_organisation(
                api::models::OrganisationIdVariant::Www,
                AVENTON_DOMAIN.to_string(),
                &context,
            )
            .await?;
        match org {
            api::GetOrganisationResponse::Ok { body: org, .. } => {
                ensure_eq!(
                    org.organisation_ids.domains,
                    vec![api::models::Id::from_str(AVENTON_DOMAIN)?],
                    "wrong domains"
                );
                ensure_eq!(
                    org.names,
                    vec![
                        api::models::ShortText {
                            text: api::models::ShortString::from_str("Aventon")?,
                            source: api::models::DataSource::Other
                        },
                        api::models::ShortText {
                            text: api::models::ShortString::from_str("Aventon Bikes")?,
                            source: api::models::DataSource::Wiki
                        },
                    ],
                    "wrong name or source"
                );
                ensure_eq!(org.medallions, vec![], "wrong certifications");
                ensure_eq!(
                    org.media,
                    vec![api::models::Medium {
                        icon: Some("https://yt3.googleusercontent.com/TAUPgsU3oOD-CYNfUo1V9rpgtH-IHbAjUdo92nusdtz9e25tLjQ_uRx0ZpnAf5DnBp6tUAQUt28=s160-c-k-c0x00ffffff-no-rj".to_string()),
                        mentions: vec![api::models::Mention {
                            link: "https://www.youtube.com/watch?v=Wx2ANP44bqQ".to_string(),
                            title: "My favorite zero waste brands and zero waste swaps I recommend for 2024".to_string(),
                        }],
                    }],
                    "wrong media"
                );
            }
            api::GetOrganisationResponse::NotFound { .. } => {
                return Err(Finding::Other(format!(
                    "Organisation {:?}:{:?} not found",
                    api::models::OrganisationIdVariant::Www,
                    MELIORA_DOMAIN,
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

        let result =
            client.search_by_text("https://meliorameansbetter.com/".to_string(), &context).await?;
        match result {
            api::SearchByTextResponse::Ok { body: org, .. } => {
                ensure_eq!(
                    org.results.len(),
                    1,
                    "found too many items when looking for meliorameansbetter.com"
                );
            }
        }

        Ok(())
    }
}
