//! This modules contains definitions of data stored in the internal database.

use std::{collections::BTreeSet, str::FromStr};

use merge::Merge;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;

#[cfg(feature = "into-api")]
use sustainity_api::models as api;

#[cfg(feature = "from-substrate")]
use sustainity_schema as schema;

use crate::ids;

pub type LibraryTopic = String;
pub type StoreGtin = usize;
pub type GatherProductId = ids::ProductId;
pub type StoreProductId = String;
pub type GatherOrganisationId = ids::OrganisationId;
pub type StoreOrganisationId = String;
pub type GatherVatId = ids::VatId;
pub type StoreVatId = String;
pub type GatherDomain = String;
pub type StoreDomain = String;

#[cfg(feature = "into-api")]
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum IntoApiError {
    #[snafu(display("Failed conversion to LibraryTopic: {err}"))]
    ToLibraryTopic { err: api::error::ConversionError },
}

#[cfg(feature = "into-api")]
impl IntoApiError {
    pub fn to_library_topic(err: api::error::ConversionError) -> Self {
        Self::ToLibraryTopic { err }
    }
}

/// Points to a source of some data.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Source {
    /// Wikidata.
    #[serde(rename = "wiki")]
    Wikidata,

    /// Open Food Facts.
    #[serde(rename = "off")]
    OpenFoodFacts,

    /// EU Ecolabel.
    #[serde(rename = "eu")]
    EuEcolabel,

    /// BCorp.
    #[serde(rename = "bcorp")]
    BCorp,

    /// Fashion Transparency Index.
    #[serde(rename = "fti")]
    Fti,

    /// TCO.
    #[serde(rename = "tco")]
    Tco,

    #[serde(rename = "other")]
    Other,
}

impl Source {
    pub fn from_string(string: &str) -> Self {
        match string {
            "wikidata" => Source::Wikidata,
            "open_food_facts" => Source::OpenFoodFacts,
            "eu_ecolabel" => Source::EuEcolabel,
            "bcorp" => Source::BCorp,
            "fti" => Source::Fti,
            "tco" => Source::Tco,
            _ => Source::Other,
        }
    }

    pub fn is_bcorp(&self) -> bool {
        matches!(self, Self::BCorp)
    }

    pub fn is_euecolabel(&self) -> bool {
        matches!(self, Self::EuEcolabel)
    }

    pub fn is_fti(&self) -> bool {
        matches!(self, Self::Fti)
    }

    pub fn is_tco(&self) -> bool {
        matches!(self, Self::Tco)
    }
}

#[cfg(feature = "into-api")]
impl Source {
    pub fn into_api(self) -> api::DataSource {
        match self {
            Self::BCorp => api::DataSource::BCorp,
            Self::EuEcolabel => api::DataSource::Eu,
            Self::Fti => api::DataSource::Fti,
            Self::OpenFoodFacts => api::DataSource::Off,
            Self::Wikidata => api::DataSource::Wiki,
            Self::Tco => api::DataSource::Tco,
            Self::Other => api::DataSource::Other,
        }
    }
}

/// Text together with it's source.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Text {
    /// Text.
    #[serde(rename = "text")]
    pub text: String,

    /// Source of the text.
    #[serde(rename = "source")]
    pub source: Source,
}

#[cfg(feature = "into-api")]
impl Text {
    pub fn into_api_long(self) -> api::LongText {
        let text = match api::LongString::from_str(&self.text) {
            Ok(ok) => ok,
            Err(err) => {
                log::error!("Could not convert to a LongString: {err}");
                default_long_string()
            }
        };

        api::LongText { text, source: self.source.into_api() }
    }

    pub fn into_api_short(self) -> api::ShortText {
        let text = match api::ShortString::from_str(&self.text) {
            Ok(ok) => ok,
            Err(err) => {
                log::error!("Could not convert to a ShortString: {err}");
                default_short_string()
            }
        };

        api::ShortText { text, source: self.source.into_api() }
    }
}

/// Image together with it's source.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Image {
    /// Name of the images.
    ///
    /// Together with the source it's possible to reconstruct images URL.
    #[serde(rename = "image")]
    pub image: String,

    /// Source of the image.
    #[serde(rename = "source")]
    pub source: Source,
}

#[cfg(feature = "into-api")]
impl Image {
    pub fn into_api(self) -> api::Image {
        api::Image { image: self.image, source: self.source.into_api() }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
#[serde(tag = "variant", content = "content")]
pub enum Regions {
    /// Available world-wide
    #[serde(rename = "all")]
    World,

    /// Region could not be identified
    #[serde(rename = "unknown")]
    #[default]
    Unknown,

    /// List of regions
    #[serde(rename = "list")]
    List(Vec<isocountry::CountryCode>),
}

impl merge::Merge for Regions {
    fn merge(&mut self, other: Self) {
        match other {
            Self::World => {
                *self = Self::World;
            }
            Self::Unknown => {}
            Self::List(other_list) => match self {
                Self::World => {}
                Self::Unknown => {
                    *self = Self::List(other_list);
                }
                Self::List(self_list) => {
                    self_list.extend(&other_list);
                    self_list.sort_unstable();
                    self_list.dedup();
                }
            },
        }
    }
}

/// Data about a `BCorp` company.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct BCorpCert {
    /// Name identifying the company.
    pub id: String,
}

#[cfg(feature = "into-api")]
impl BCorpCert {
    pub fn into_api(self) -> api::Medallion {
        let bcorp = match api::Id::from_str(&self.id) {
            Ok(id) => Some(api::BCorpMedallion { id }),
            Err(err) => {
                log::error!("Could not convert Id: {err}");
                None
            }
        };

        api::Medallion {
            variant: api::MedallionVariant::BCorp,
            bcorp,
            eu_ecolabel: None,
            fti: None,
            sustainity: None,
            tco: None,
        }
    }
}

/// Data about a company certified by EU Ecolabel.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct EuEcolabelCert;

#[cfg(feature = "into-api")]
impl EuEcolabelCert {
    pub fn into_api(self) -> api::Medallion {
        api::Medallion {
            variant: api::MedallionVariant::EuEcolabel,
            bcorp: None,
            eu_ecolabel: Some(api::EuEcolabelMedallion { match_accuracy: None }),
            fti: None,
            sustainity: None,
            tco: None,
        }
    }
}

/// Data about a company scored by Fashion Transparency Index.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct FtiCert {
    /// Score (from 0% to 100%).
    pub score: i64,
}

#[cfg(feature = "into-api")]
impl FtiCert {
    pub fn into_api(self) -> api::Medallion {
        api::Medallion {
            variant: api::MedallionVariant::Fti,
            bcorp: None,
            eu_ecolabel: None,
            fti: Some(api::FtiMedallion { score: self.score }),
            sustainity: None,
            tco: None,
        }
    }
}

/// Data about a company which products were certified by TCO.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct TcoCert {
    /// Name identifying the company.
    pub brand_name: String,
}

#[cfg(feature = "into-api")]
impl TcoCert {
    pub fn into_api(self) -> api::Medallion {
        let tco = match api::ShortString::from_str(&self.brand_name) {
            Ok(brand_name) => Some(api::TcoMedallion { brand_name }),
            Err(err) => {
                log::error!("Could not convert a brand name to a ShortString: {err}");
                None
            }
        };

        api::Medallion {
            variant: api::MedallionVariant::Tco,
            bcorp: None,
            eu_ecolabel: None,
            fti: None,
            sustainity: None,
            tco,
        }
    }
}

/// Lists known certifications.
#[derive(Serialize, Deserialize, Debug, Clone, Default, Eq, PartialEq, Merge)]
pub struct Certifications {
    /// Manufacturer certifiad by BCorp.
    pub bcorp: Option<BCorpCert>,

    /// Manufacturer certified by EU Ecolabel.
    pub eu_ecolabel: Option<EuEcolabelCert>,

    /// Organisation scored by Fashion Transparency Index.
    pub fti: Option<FtiCert>,

    /// Manufacturer certifiad by TCO.
    pub tco: Option<TcoCert>,
}

impl Certifications {
    /// Returns number of given certifications.
    ///
    /// TODO: FTI is not a certification.
    #[must_use]
    pub fn get_num(&self) -> usize {
        usize::from(self.bcorp.is_some())
            + usize::from(self.eu_ecolabel.is_some())
            + usize::from(self.fti.is_some())
            + usize::from(self.tco.is_some())
    }

    /// Copies certifications.
    ///
    /// EU Ecolabel is not inherited - this certification is assigned directly to products, not companies.
    pub fn inherit(&mut self, other: &Self) {
        if other.bcorp.is_some() {
            self.bcorp.clone_from(&other.bcorp);
        }
        if other.fti.is_some() {
            self.fti.clone_from(&other.fti);
        }
        if other.tco.is_some() {
            self.tco.clone_from(&other.tco);
        }
    }
}

#[cfg(feature = "into-api")]
impl Certifications {
    pub fn into_api_medallions(self) -> Vec<api::Medallion> {
        let mut medallions = Vec::new();
        if let Some(bcorp) = self.bcorp {
            medallions.push(bcorp.into_api());
        }
        if let Some(eu_ecolabel) = self.eu_ecolabel {
            medallions.push(eu_ecolabel.into_api());
        }
        if let Some(fti) = self.fti {
            medallions.push(fti.into_api());
        }
        if let Some(tco) = self.tco {
            medallions.push(tco.into_api());
        }
        medallions
    }

    pub fn to_api_badges(&self) -> Vec<api::BadgeName> {
        let mut badges = Vec::new();
        if self.bcorp.is_some() {
            badges.push(api::BadgeName::Bcorp);
        }
        if self.eu_ecolabel.is_some() {
            badges.push(api::BadgeName::Eu);
        }
        if self.tco.is_some() {
            badges.push(api::BadgeName::Tco);
        }
        badges
    }

    pub fn to_api_scores(&self) -> Vec<api::Score> {
        let mut scores = Vec::with_capacity(1);
        if let Some(fti) = &self.fti {
            scores.push(api::Score { scorer_name: api::ScorerName::Fti, score: fti.score });
        }
        scores
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct IdEntry {
    /// DB entry key.
    #[serde(rename = "_key")]
    pub db_key: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct Keyword {
    /// DB entry ID.
    #[serde(rename = "_key")]
    pub db_key: String,

    /// The keyword value.
    #[serde(rename = "keyword")]
    pub keyword: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SustainityScoreCategory {
    #[serde(rename = "root")]
    Root,

    #[serde(rename = "data_availability")]
    DataAvailability,

    #[serde(rename = "producer_known")]
    ProducerKnown,

    #[serde(rename = "production_place_known")]
    ProductionPlaceKnown,

    #[serde(rename = "id_known")]
    IdKnown,

    #[serde(rename = "category_assigned")]
    CategoryAssigned,

    #[serde(rename = "category")]
    Category,

    #[serde(rename = "warranty_length")]
    WarrantyLength,

    #[serde(rename = "num_certs")]
    NumCerts,

    #[serde(rename = "at_least_one_cert")]
    AtLeastOneCert,

    #[serde(rename = "at_least_two_certs")]
    AtLeastTwoCerts,
}

#[cfg(feature = "into-api")]
impl SustainityScoreCategory {
    pub fn into_api(self) -> api::SustainityScoreCategory {
        match self {
            Self::Root => unimplemented!(), //< This category is never passed to the API
            Self::DataAvailability => api::SustainityScoreCategory::DataAvailability,
            Self::ProducerKnown => api::SustainityScoreCategory::ProducerKnown,
            Self::ProductionPlaceKnown => api::SustainityScoreCategory::ProductionPlaceKnown,
            Self::IdKnown => api::SustainityScoreCategory::IdKnown,
            Self::CategoryAssigned => api::SustainityScoreCategory::CategoryAssigned,
            Self::Category => api::SustainityScoreCategory::Category,
            Self::WarrantyLength => api::SustainityScoreCategory::WarrantyLength,
            Self::NumCerts => api::SustainityScoreCategory::NumCerts,
            Self::AtLeastOneCert => api::SustainityScoreCategory::AtLeastOneCert,
            Self::AtLeastTwoCerts => api::SustainityScoreCategory::AtLeastTwoCerts,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SustainityScoreBranch {
    /// Subbranches of the tree.
    #[serde(rename = "branches")]
    pub branches: Vec<SustainityScoreBranch>,

    /// Category representing this branch.
    #[serde(rename = "category")]
    pub category: SustainityScoreCategory,

    /// Weight of this branch.
    #[serde(rename = "weight")]
    pub weight: i32,

    /// Calculated subscore of this branch.
    #[serde(rename = "score")]
    pub score: f64,
}

#[cfg(feature = "into-api")]
impl SustainityScoreBranch {
    pub fn into_api(self) -> api::SustainityScoreBranch {
        api::SustainityScoreBranch {
            branches: self.branches.into_iter().map(|b| b.into_api()).collect(),
            category: self.category.into_api(),
            weight: self.weight as i64,
            score: self.score,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SustainityScore {
    /// Score tree.
    #[serde(rename = "tree")]
    pub tree: Vec<SustainityScoreBranch>,

    /// Total calculated score.
    #[serde(rename = "total")]
    pub total: f64,
}

#[cfg(feature = "into-api")]
impl SustainityScore {
    pub fn into_api_score(self) -> api::SustainityScore {
        api::SustainityScore {
            tree: self.tree.into_iter().map(|t| t.into_api()).collect(),
            total: self.total,
        }
    }

    fn into_api_medallion(self) -> api::Medallion {
        api::Medallion {
            variant: api::MedallionVariant::Sustainity,
            sustainity: Some(api::SustainityMedallion { score: self.into_api_score() }),
            bcorp: None,
            eu_ecolabel: None,
            fti: None,
            tco: None,
        }
    }
}

impl Default for SustainityScore {
    fn default() -> Self {
        Self { tree: Vec::default(), total: 0.0 }
    }
}

/// Represents an edge in a graph database.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct Edge {
    /// The "from" vertex ID.
    #[serde(rename = "_from")]
    pub from: String,

    /// The "to" vertex ID.
    #[serde(rename = "_to")]
    pub to: String,
}

/// Represents a set of IDs of an organisation.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct GatherOrganisationIds {
    /// VAT IDs.
    pub vat_ids: BTreeSet<GatherVatId>,

    /// Organisation ID.
    pub wiki: BTreeSet<ids::WikiId>,

    /// Web domains.
    pub domains: BTreeSet<GatherDomain>,
}

impl GatherOrganisationIds {
    pub fn store(self) -> StoreOrganisationIds {
        let mut vat_ids: Vec<String> =
            self.vat_ids.into_iter().map(|id| id.as_str().to_owned()).collect();
        let mut wiki: Vec<String> =
            self.wiki.into_iter().map(|id| id.get_value().to_string()).collect();
        let mut domains: Vec<String> = self.domains.into_iter().collect();

        vat_ids.sort();
        wiki.sort();
        domains.sort();

        StoreOrganisationIds { vat_ids, wiki, domains }
    }
}

impl merge::Merge for GatherOrganisationIds {
    fn merge(&mut self, other: Self) {
        self.wiki.extend(other.wiki);
        self.vat_ids.extend(other.vat_ids);
        self.domains.extend(other.domains);
    }
}

#[cfg(feature = "from-substrate")]
impl TryFrom<schema::ProducerIds> for GatherOrganisationIds {
    type Error = ids::ParseIdError;

    fn try_from(ids: schema::ProducerIds) -> Result<GatherOrganisationIds, Self::Error> {
        let mut vat_ids = BTreeSet::<GatherVatId>::new();
        if let Some(ids) = ids.vat {
            for id in ids {
                vat_ids.insert(GatherVatId::try_from(&id)?);
            }
        }

        let mut wiki = BTreeSet::<ids::WikiId>::new();
        if let Some(ids) = ids.wiki {
            for id in ids {
                wiki.insert(ids::WikiId::try_from(&id)?);
            }
        }

        let mut domains = BTreeSet::<GatherDomain>::new();
        if let Some(ids) = ids.domains {
            for id in ids {
                domains.insert(id);
            }
        }

        Ok(Self { vat_ids, wiki, domains })
    }
}

/// Represents a set of IDs of an organisation.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct StoreOrganisationIds {
    /// Organisation ID.
    #[serde(rename = "wiki")]
    pub wiki: Vec<String>,

    /// VAT IDs.
    #[serde(rename = "vat_ids")]
    pub vat_ids: Vec<StoreVatId>,

    /// Web domains.
    #[serde(rename = "domains")]
    pub domains: Vec<StoreDomain>,
}

#[allow(clippy::ptr_arg)]
fn str_to_id(s: &String) -> api::Id {
    api::Id::from_str(s).expect("Converting IDs")
}

#[cfg(feature = "into-api")]
impl StoreOrganisationIds {
    pub fn to_api(self) -> api::OrganisationIds {
        api::OrganisationIds {
            wiki: self.wiki.iter().map(str_to_id).collect(),
            vat: self.vat_ids.iter().map(str_to_id).collect(),
            domains: self.domains.iter().map(str_to_id).collect(),
        }
    }
}

/// Represents an organisation (e.g. manufacturer, shop).
#[derive(Debug, Clone)]
pub struct GatherOrganisation {
    /// DB entry ID.
    pub db_key: GatherOrganisationId,

    /// Organisation IDs.
    pub ids: GatherOrganisationIds,

    /// Names of the organisation.
    pub names: BTreeSet<Text>,

    /// Descriptions of the organisation.
    pub descriptions: BTreeSet<Text>,

    /// Logo images.
    pub images: BTreeSet<Image>,

    /// Websites.
    pub websites: BTreeSet<String>,

    /// Known certifications.
    pub certifications: Certifications,
}

impl GatherOrganisation {
    pub fn store(self) -> StoreOrganisation {
        let db_key = self.db_key.to_string();
        let ids = self.ids.store();
        let mut names: Vec<_> = self.names.into_iter().collect();
        let mut descriptions: Vec<_> = self.descriptions.into_iter().collect();
        let mut images: Vec<_> = self.images.into_iter().collect();
        let mut websites: Vec<_> = self.websites.into_iter().collect();
        let certifications = self.certifications;

        names.sort();
        descriptions.sort();
        images.sort();
        websites.sort();

        StoreOrganisation { db_key, ids, names, descriptions, images, websites, certifications }
    }
}

impl merge::Merge for GatherOrganisation {
    fn merge(&mut self, other: Self) {
        self.ids.merge(other.ids);
        self.names.extend(other.names);
        self.descriptions.extend(other.descriptions);
        self.images.extend(other.images);
        self.websites.extend(other.websites);
        self.certifications.merge(other.certifications);
    }
}

/// Represents an organisation (e.g. manufacturer, shop).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StoreOrganisation {
    /// DB entry ID.
    #[serde(rename = "_key")]
    pub db_key: StoreOrganisationId,

    /// Organisation IDs.
    #[serde(rename = "ids")]
    pub ids: StoreOrganisationIds,

    /// Names of the organisation.
    #[serde(rename = "names")]
    pub names: Vec<Text>,

    /// Descriptions of the organisation.
    #[serde(rename = "descriptions")]
    pub descriptions: Vec<Text>,

    /// Logo images.
    #[serde(rename = "images")]
    pub images: Vec<Image>,

    /// Websites.
    #[serde(rename = "websites")]
    pub websites: Vec<String>,

    /// Known certifications.
    #[serde(rename = "certifications")]
    pub certifications: Certifications,
}

fn default_short_string() -> api::ShortString {
    api::ShortString::from_str("").expect("ShortString from an empty string")
}

fn default_long_string() -> api::LongString {
    api::LongString::from_str("").expect("LongString from an empty string")
}

fn str_to_short_string(s: String) -> api::ShortString {
    api::ShortString::from_str(&s).expect("Converting strings")
}

fn str_to_long_string(s: String) -> api::LongString {
    api::LongString::from_str(&s).expect("Converting strings")
}

fn text_to_short_string(text: &Text) -> api::ShortString {
    api::ShortString::from_str(&text.text).expect("Converting texts")
}

fn text_to_long_string(text: &Text) -> api::LongString {
    api::LongString::from_str(&text.text).expect("Converting texts")
}

#[cfg(feature = "into-api")]
impl StoreOrganisation {
    pub fn into_api_short(self) -> api::OrganisationShort {
        api::OrganisationShort {
            organisation_ids: self.ids.to_api(),
            name: self.names.first().map_or_else(default_short_string, text_to_short_string),
            description: self.descriptions.first().map(text_to_long_string),
            badges: self.certifications.to_api_badges(),
            scores: self.certifications.to_api_scores(),
        }
    }

    pub fn into_api_full(self, products: Vec<api::ProductShort>) -> api::OrganisationFull {
        api::OrganisationFull {
            organisation_ids: self.ids.to_api(),
            names: self.names.into_iter().map(|n| n.into_api_short()).collect(),
            descriptions: self.descriptions.into_iter().map(|d| d.into_api_long()).collect(),
            images: self.images.into_iter().map(|i| i.into_api()).collect(),
            websites: self.websites.into_iter().map(str_to_short_string).collect(),
            medallions: self.certifications.into_api_medallions(),
            products,
        }
    }
}

/// Represents a set of product IDs.
#[derive(Serialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct GatherProductIds {
    /// GTIN of the product.
    #[serde(rename = "eans")]
    pub eans: BTreeSet<ids::Ean>,

    /// GTIN of the product.
    #[serde(rename = "gtins")]
    pub gtins: BTreeSet<ids::Gtin>,

    /// Wiki ID.
    #[serde(rename = "wiki")]
    pub wiki: BTreeSet<ids::WikiId>,
}

impl GatherProductIds {
    pub fn is_empty(&self) -> bool {
        self.eans.is_empty() && self.gtins.is_empty() && self.wiki.is_empty()
    }

    pub fn store(self) -> StoreProductIds {
        let mut eans: Vec<_> = self.eans.into_iter().map(|id| id.to_string()).collect();
        let mut gtins: Vec<_> = self.gtins.into_iter().map(|id| id.to_string()).collect();
        let mut wiki: Vec<_> = self.wiki.into_iter().map(|id| id.get_value().to_string()).collect();

        eans.sort();
        gtins.sort();
        wiki.sort();

        StoreProductIds { eans, gtins, wiki }
    }
}

impl merge::Merge for GatherProductIds {
    fn merge(&mut self, other: Self) {
        self.eans.extend(other.eans);
        self.gtins.extend(other.gtins);
        self.wiki.extend(other.wiki);
    }
}

#[cfg(feature = "from-substrate")]
impl TryFrom<schema::ProductIds> for GatherProductIds {
    type Error = ids::ParseIdError;

    fn try_from(ids: schema::ProductIds) -> Result<Self, Self::Error> {
        let mut eans = BTreeSet::<ids::Ean>::new();
        if let Some(ids) = ids.ean {
            for id in ids {
                eans.insert(ids::Ean::try_from(&id)?);
            }
        }

        let mut gtins = BTreeSet::<ids::Gtin>::new();
        if let Some(ids) = ids.gtin {
            for id in ids {
                gtins.insert(ids::Gtin::try_from(&id)?);
            }
        }

        let mut wiki = BTreeSet::<ids::WikiId>::new();
        if let Some(ids) = ids.wiki {
            for id in ids {
                wiki.insert(ids::WikiId::try_from(&id)?);
            }
        }

        Ok(Self { eans, gtins, wiki })
    }
}

/// Represents a set of product IDs.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct StoreProductIds {
    /// GTIN of the product.
    #[serde(rename = "eans")]
    pub eans: Vec<String>,

    /// GTIN of the product.
    #[serde(rename = "gtins")]
    pub gtins: Vec<String>,

    /// Wiki ID.
    #[serde(rename = "wiki")]
    pub wiki: Vec<String>,
}

#[cfg(feature = "into-api")]
impl StoreProductIds {
    pub fn to_api(self) -> api::ProductIds {
        api::ProductIds {
            eans: self.eans.iter().map(str_to_id).collect(),
            gtins: self.gtins.iter().map(str_to_id).collect(),
            wiki: self.wiki.iter().map(str_to_id).collect(),
        }
    }
}

/// Represents a product.
#[derive(Debug, Clone)]
pub struct GatherProduct {
    /// DB entry ID.
    pub db_key: GatherProductId,

    /// Product ID.
    pub ids: GatherProductIds,

    /// Names of the product.
    pub names: BTreeSet<Text>,

    /// Descriptions of the product.
    pub descriptions: BTreeSet<Text>,

    /// Product images.
    pub images: BTreeSet<Image>,

    /// Product categories.
    pub categories: BTreeSet<String>,

    /// Regions where the product is available.
    pub regions: Regions,

    /// Known certifications.
    pub certifications: Certifications,

    /// DB IDs of manufacturers.
    pub manufacturer_ids: BTreeSet<GatherOrganisationId>,

    /// Wikidata IDs newer version products.
    pub follows: BTreeSet<GatherProductId>,

    /// Wikidata IDs older version products.
    pub followed_by: BTreeSet<GatherProductId>,

    /// The Sustainity score.
    pub sustainity_score: SustainityScore,
}

impl GatherProduct {
    pub fn store(self) -> StoreProduct {
        let db_key = self.db_key.to_string();
        let ids = self.ids.store();
        let mut names: Vec<_> = self.names.into_iter().collect();
        let descriptions = self.descriptions.into_iter().collect();
        let mut images: Vec<_> = self.images.into_iter().collect();
        let mut categories: Vec<_> = self.categories.into_iter().collect();
        let regions = self.regions;
        let certifications = self.certifications;
        let mut manufacturer_ids: Vec<_> =
            self.manufacturer_ids.into_iter().map(|id| id.to_string()).collect();
        let mut follows: Vec<_> = self.follows.into_iter().map(|id| id.to_string()).collect();
        let mut followed_by: Vec<_> =
            self.followed_by.into_iter().map(|id| id.to_string()).collect();
        let sustainity_score = self.sustainity_score;

        names.sort();
        images.sort();
        categories.sort();
        manufacturer_ids.sort();
        follows.sort();
        followed_by.sort();

        StoreProduct {
            db_key,
            ids,
            names,
            descriptions,
            images,
            categories,
            regions,
            certifications,
            manufacturer_ids,
            follows,
            followed_by,
            sustainity_score,
        }
    }
}

impl merge::Merge for GatherProduct {
    fn merge(&mut self, other: Self) {
        self.ids.merge(other.ids);
        self.names.extend(other.names);
        self.descriptions.extend(other.descriptions);
        self.images.extend(other.images);
        self.categories.extend(other.categories);
        self.regions.merge(other.regions);
        self.certifications.merge(other.certifications);
        self.manufacturer_ids.extend(other.manufacturer_ids);
        self.follows.extend(other.follows);
        self.followed_by.extend(other.followed_by);
    }
}

/// Represents a product.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StoreProduct {
    /// DB entry ID.
    #[serde(rename = "_key")]
    pub db_key: StoreProductId,

    /// Product ID.
    #[serde(rename = "ids")]
    pub ids: StoreProductIds,

    /// Names of the product.
    #[serde(rename = "names")]
    pub names: Vec<Text>,

    /// Descriptions of the product.
    #[serde(rename = "descriptions")]
    pub descriptions: Vec<Text>,

    /// Product images.
    #[serde(rename = "images")]
    pub images: Vec<Image>,

    /// Product categories.
    #[serde(rename = "categories")]
    pub categories: Vec<String>,

    /// Regions where the product is available.
    #[serde(rename = "regions")]
    pub regions: Regions,

    /// Known certifications.
    #[serde(rename = "certifications")]
    pub certifications: Certifications,

    /// DB IDs of manufacturers.
    #[serde(rename = "manufacturer_ids")]
    pub manufacturer_ids: Vec<StoreOrganisationId>,

    /// Wikidata IDs newer version products.
    #[serde(rename = "follows")]
    pub follows: Vec<StoreProductId>,

    /// Wikidata IDs older version products.
    #[serde(rename = "followed_by")]
    pub followed_by: Vec<StoreProductId>,

    /// The Sustainity score.
    #[serde(rename = "sustainity_score")]
    pub sustainity_score: SustainityScore,
}

#[cfg(feature = "into-api")]
impl StoreProduct {
    pub fn into_api_short(self) -> api::ProductShort {
        api::ProductShort {
            product_ids: self.ids.to_api(),
            name: self.names.first().map_or_else(default_short_string, text_to_short_string),
            description: self.descriptions.first().map(text_to_long_string),
            badges: self.certifications.to_api_badges(),
            scores: self.certifications.to_api_scores(),
        }
    }

    pub fn into_api_full(
        self,
        manufacturers: Vec<api::OrganisationShort>,
        alternatives: Vec<api::CategoryAlternatives>,
    ) -> api::ProductFull {
        let mut medallions = self.certifications.into_api_medallions();
        medallions.push(self.sustainity_score.into_api_medallion());

        api::ProductFull {
            product_ids: self.ids.to_api(),
            names: self.names.into_iter().map(|n| n.into_api_short()).collect(),
            descriptions: self.descriptions.into_iter().map(|d| d.into_api_long()).collect(),
            images: self.images.into_iter().map(|i| i.into_api()).collect(),
            manufacturers,
            alternatives,
            medallions,
        }
    }
}

/// One enttry in `PresentationData::Scored`.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ScoredPresentationEntry<W> {
    /// Organisation ID.
    #[serde(rename = "wiki_id")]
    pub wiki_id: W,

    /// Name of the organisation (as originally listed by the certifier).
    #[serde(rename = "name")]
    pub name: String,

    /// Score from the certifier.
    #[serde(rename = "score")]
    pub score: i64,
}

pub type GatherScoredPresentationEntry = ScoredPresentationEntry<ids::WikiId>;
pub type StoreScoredPresentationEntry = ScoredPresentationEntry<String>;

#[cfg(feature = "into-api")]
impl StoreScoredPresentationEntry {
    pub fn into_api(self) -> api::PresentationEntry {
        api::PresentationEntry {
            wiki_id: api::Id::from_str(&self.wiki_id).expect("Converting to Wikidata ID"),
            name: str_to_short_string(self.name),
            score: self.score,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum PresentationData<O> {
    Scored(Vec<ScoredPresentationEntry<O>>),
}

pub type GatherPresentationData = PresentationData<ids::WikiId>;
pub type StorePresentationData = PresentationData<String>;

#[cfg(feature = "into-api")]
impl StorePresentationData {
    fn into_api(self) -> Vec<api::PresentationEntry> {
        match self {
            PresentationData::Scored(entries) => {
                entries.into_iter().map(|e| e.into_api()).collect()
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Presentation<O> {
    /// Topic ID.
    pub id: LibraryTopic,

    /// Data to be presented.
    pub data: PresentationData<O>,
}

pub type GatherPresentation = Presentation<ids::WikiId>;
pub type StorePresentation = Presentation<String>;

#[cfg(feature = "into-api")]
impl StorePresentation {
    pub fn into_api(self) -> api::Presentation {
        api::Presentation { data: self.data.into_api() }
    }
}

/// Represents a topic info.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LibraryItem {
    /// Topic ID.
    pub id: LibraryTopic,

    /// Article title.
    pub title: String,

    /// Short one line summary of the article.
    pub summary: String,

    /// Contents of the article in markdown format.
    pub article: String,
}

#[cfg(feature = "into-api")]
impl LibraryItem {
    pub fn try_into_api_short(self) -> Result<api::LibraryItemShort, IntoApiError> {
        Ok(api::LibraryItemShort {
            id: api::LibraryTopic::from_str(&self.id).map_err(IntoApiError::to_library_topic)?,
            title: str_to_short_string(self.title),
            summary: str_to_short_string(self.summary),
        })
    }

    pub fn try_into_api_full(
        self,
        presentation: Option<api::Presentation>,
    ) -> Result<api::LibraryItemFull, IntoApiError> {
        Ok(api::LibraryItemFull {
            id: api::LibraryTopic::from_str(&self.id).map_err(IntoApiError::to_library_topic)?,
            title: str_to_short_string(self.title),
            summary: str_to_short_string(self.summary),
            article: str_to_long_string(self.article),
            presentation,
        })
    }
}
