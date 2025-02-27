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
pub type Domain = String;

#[cfg(feature = "into-api")]
#[allow(clippy::ptr_arg)]
fn domain_to_id(s: &String) -> api::Id {
    api::Id::from_str(s).expect("Converting a domain")
}

#[cfg(feature = "into-api")]
fn wiki_to_id(id: &ids::WikiId) -> api::Id {
    api::Id::from_str(&id.to_canonical_string()).expect("Converting Wiki ID")
}

#[cfg(feature = "into-api")]
fn vat_to_id(id: &ids::VatId) -> api::Id {
    api::Id::from_str(&id.to_canonical_string()).expect("Converting Vat ID")
}

#[cfg(feature = "into-api")]
fn ean_to_id(id: &ids::Ean) -> api::Id {
    api::Id::from_str(&id.to_canonical_string()).expect("Converting EAN")
}

#[cfg(feature = "into-api")]
fn gtin_to_id(id: &ids::Gtin) -> api::Id {
    api::Id::from_str(&id.to_canonical_string()).expect("Converting GTIN")
}

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
#[repr(u8)]
pub enum Source {
    /// Wikidata.
    Wikidata,

    /// Open Food Facts.
    OpenFoodFacts,

    /// EU Ecolabel.
    EuEcolabel,

    /// BCorp.
    BCorp,

    /// Fashion Transparency Index.
    Fti,

    /// TCO.
    Tco,

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
    pub text: String,

    /// Source of the text.
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
    pub image: String,

    /// Source of the image.
    pub source: Source,
}

#[cfg(feature = "into-api")]
impl Image {
    pub fn into_api(self) -> api::Image {
        api::Image { image: self.image, source: self.source.into_api() }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum Regions {
    /// Available world-wide
    World,

    /// Region could not be identified
    Unknown,

    /// List of regions
    List(Vec<isocountry::CountryCode>),
}

impl Regions {
    pub fn is_available_in(&self, region: Option<&str>) -> bool {
        match self {
            Self::World => true,
            Self::Unknown => false,
            Self::List(codes) => region
                .map(|region| codes.iter().any(|code| code.alpha3() == region))
                .unwrap_or(false),
        }
    }
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

#[derive(Serialize, Deserialize, Debug, Clone)]
#[repr(u8)]
pub enum SustainityScoreCategory {
    Root,
    DataAvailability,
    ProducerKnown,
    ProductionPlaceKnown,
    IdKnown,
    CategoryAssigned,
    Category,
    WarrantyLength,
    NumCerts,
    AtLeastOneCert,
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
    pub branches: Vec<SustainityScoreBranch>,

    /// Category representing this branch.
    pub category: SustainityScoreCategory,

    /// Weight of this branch.
    pub weight: i32,

    /// Calculated subscore of this branch.
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
    pub tree: Vec<SustainityScoreBranch>,

    /// Total calculated score.
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

/// Represents a set of IDs of an organisation.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct GatherOrganisationIds {
    /// VAT IDs.
    pub vat_ids: BTreeSet<ids::VatId>,

    /// Organisation ID.
    pub wiki: BTreeSet<ids::WikiId>,

    /// Web domains.
    pub domains: BTreeSet<Domain>,
}

impl GatherOrganisationIds {
    pub fn store(self) -> StoreOrganisationIds {
        let mut vat_ids: Vec<_> = self.vat_ids.into_iter().collect();
        let mut wiki: Vec<_> = self.wiki.into_iter().collect();
        let mut domains: Vec<_> = self.domains.into_iter().collect();

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

    fn try_from(ids: schema::ProducerIds) -> Result<Self, Self::Error> {
        let mut vat_ids = BTreeSet::<ids::VatId>::new();
        if let Some(ids) = ids.vat {
            for id in ids {
                vat_ids.insert(ids::VatId::try_from(&id)?);
            }
        }

        let mut wiki = BTreeSet::<ids::WikiId>::new();
        if let Some(ids) = ids.wiki {
            for id in ids {
                wiki.insert(ids::WikiId::try_from(&id)?);
            }
        }

        let mut domains = BTreeSet::<Domain>::new();
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
    pub wiki: Vec<ids::WikiId>,

    /// VAT IDs.
    pub vat_ids: Vec<ids::VatId>,

    /// Web domains.
    pub domains: Vec<Domain>,
}

#[cfg(feature = "into-api")]
impl StoreOrganisationIds {
    pub fn to_api(self) -> api::OrganisationIds {
        api::OrganisationIds {
            wiki: self.wiki.iter().map(wiki_to_id).collect(),
            vat: self.vat_ids.iter().map(vat_to_id).collect(),
            domains: self.domains.iter().map(domain_to_id).collect(),
        }
    }
}

/// Represents an organisation (e.g. manufacturer, shop).
#[derive(Debug, Clone)]
pub struct GatherOrganisation {
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

    /// Products of this organistion.
    pub products: BTreeSet<ids::ProductId>,

    /// Known certifications.
    pub certifications: Certifications,
}

impl GatherOrganisation {
    pub fn store(self) -> StoreOrganisation {
        let ids = self.ids.store();
        let mut names: Vec<_> = self.names.into_iter().collect();
        let mut descriptions: Vec<_> = self.descriptions.into_iter().collect();
        let mut images: Vec<_> = self.images.into_iter().collect();
        let mut websites: Vec<_> = self.websites.into_iter().collect();
        let mut products: Vec<_> = self.products.into_iter().collect();
        let certifications = self.certifications;

        names.sort();
        descriptions.sort();
        images.sort();
        products.sort();
        websites.sort();

        StoreOrganisation { ids, names, descriptions, images, websites, products, certifications }
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
    /// Organisation IDs.
    pub ids: StoreOrganisationIds,

    /// Names of the organisation.
    pub names: Vec<Text>,

    /// Descriptions of the organisation.
    pub descriptions: Vec<Text>,

    /// Logo images.
    pub images: Vec<Image>,

    /// Websites.
    pub websites: Vec<String>,

    /// Products of this organistion.
    pub products: Vec<ids::ProductId>,

    /// Known certifications.
    pub certifications: Certifications,
}

#[cfg(feature = "into-api")]
fn default_short_string() -> api::ShortString {
    api::ShortString::from_str("").expect("ShortString from an empty string")
}

#[cfg(feature = "into-api")]
fn default_long_string() -> api::LongString {
    api::LongString::from_str("").expect("LongString from an empty string")
}

#[cfg(feature = "into-api")]
fn str_to_short_string(s: String) -> api::ShortString {
    api::ShortString::from_str(&s).expect("Converting strings")
}

#[cfg(feature = "into-api")]
fn str_to_long_string(s: String) -> api::LongString {
    api::LongString::from_str(&s).expect("Converting strings")
}

#[cfg(feature = "into-api")]
fn text_to_short_string(text: &Text) -> api::ShortString {
    api::ShortString::from_str(&text.text).expect("Converting texts")
}

#[cfg(feature = "into-api")]
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
    pub eans: BTreeSet<ids::Ean>,

    /// GTIN of the product.
    pub gtins: BTreeSet<ids::Gtin>,

    /// Wiki ID.
    pub wiki: BTreeSet<ids::WikiId>,
}

impl GatherProductIds {
    pub fn is_empty(&self) -> bool {
        self.eans.is_empty() && self.gtins.is_empty() && self.wiki.is_empty()
    }

    pub fn store(self) -> StoreProductIds {
        let mut eans: Vec<_> = self.eans.into_iter().collect();
        let mut gtins: Vec<_> = self.gtins.into_iter().collect();
        let mut wiki: Vec<_> = self.wiki.into_iter().collect();

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
    pub eans: Vec<ids::Ean>,

    /// GTIN of the product.
    pub gtins: Vec<ids::Gtin>,

    /// Wiki ID.
    pub wiki: Vec<ids::WikiId>,
}

#[cfg(feature = "into-api")]
impl StoreProductIds {
    pub fn to_api(self) -> api::ProductIds {
        api::ProductIds {
            eans: self.eans.iter().map(ean_to_id).collect(),
            gtins: self.gtins.iter().map(gtin_to_id).collect(),
            wiki: self.wiki.iter().map(wiki_to_id).collect(),
        }
    }
}

/// Represents a product.
#[derive(Debug, Clone)]
pub struct GatherProduct {
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
    pub manufacturers: BTreeSet<ids::OrganisationId>,

    /// Wikidata IDs newer version products.
    pub follows: BTreeSet<ids::ProductId>,

    /// Wikidata IDs older version products.
    pub followed_by: BTreeSet<ids::ProductId>,

    /// The Sustainity score.
    pub sustainity_score: SustainityScore,
}

impl GatherProduct {
    pub fn store(self) -> StoreProduct {
        let ids = self.ids.store();
        let mut names: Vec<_> = self.names.into_iter().collect();
        let descriptions = self.descriptions.into_iter().collect();
        let mut images: Vec<_> = self.images.into_iter().collect();
        let mut categories: Vec<_> = self.categories.into_iter().collect();
        let regions = self.regions;
        let certifications = self.certifications;
        let mut manufacturers: Vec<_> = self.manufacturers.into_iter().collect();
        let mut follows: Vec<_> = self.follows.into_iter().collect();
        let mut followed_by: Vec<_> = self.followed_by.into_iter().collect();
        let sustainity_score = self.sustainity_score;

        names.sort();
        images.sort();
        categories.sort();
        manufacturers.sort();
        follows.sort();
        followed_by.sort();

        StoreProduct {
            ids,
            names,
            descriptions,
            images,
            categories,
            regions,
            certifications,
            manufacturers,
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
        self.manufacturers.extend(other.manufacturers);
        self.follows.extend(other.follows);
        self.followed_by.extend(other.followed_by);
    }
}

/// Represents a product.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StoreProduct {
    /// Product ID.
    pub ids: StoreProductIds,

    /// Names of the product.
    pub names: Vec<Text>,

    /// Descriptions of the product.
    pub descriptions: Vec<Text>,

    /// Product images.
    pub images: Vec<Image>,

    /// Product categories.
    pub categories: Vec<String>,

    /// Regions where the product is available.
    pub regions: Regions,

    /// Known certifications.
    pub certifications: Certifications,

    /// DB IDs of manufacturers.
    pub manufacturers: Vec<ids::OrganisationId>,

    /// Wikidata IDs newer version products.
    pub follows: Vec<ids::ProductId>,

    /// Wikidata IDs older version products.
    pub followed_by: Vec<ids::ProductId>,

    /// The Sustainity score.
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

    pub fn score(&self) -> f64 {
        0.0 + 0.9 * self.certifications.bcorp.is_some() as u32 as f64
            + 0.9 * self.certifications.eu_ecolabel.is_some() as u32 as f64
            + 0.6 * self.certifications.fti.as_ref().map(|c| 0.01 * c.score as f64).unwrap_or(0.0)
            + 0.3 * self.certifications.tco.is_some() as u32 as f64
    }
}

/// One enttry in `PresentationData::Scored`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ScoredPresentationEntry {
    /// Organisation ID.
    pub wiki_id: ids::WikiId,

    /// Name of the organisation (as originally listed by the certifier).
    pub name: String,

    /// Score from the certifier.
    pub score: i64,
}

#[cfg(feature = "into-api")]
impl ScoredPresentationEntry {
    pub fn into_api(self) -> api::PresentationEntry {
        api::PresentationEntry {
            wiki_id: api::Id::from_str(&self.wiki_id.to_canonical_string())
                .expect("Converting to Wikidata ID"),
            name: str_to_short_string(self.name),
            score: self.score,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct PresentationData {
    pub entries: Vec<ScoredPresentationEntry>,
}

#[cfg(feature = "into-api")]
impl PresentationData {
    fn into_api(self) -> Vec<api::PresentationEntry> {
        self.entries.into_iter().map(|e| e.into_api()).collect()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Presentation {
    /// Topic ID.
    pub id: LibraryTopic,

    /// Data to be presented.
    pub data: PresentationData,
}

#[cfg(feature = "into-api")]
impl Presentation {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_presentation_with_scored_data_json() {
        let original_presentation = Presentation {
            id: "topic".to_owned(),
            data: PresentationData {
                entries: vec![
                    ScoredPresentationEntry {
                        wiki_id: ids::WikiId::new(1),
                        name: "name1".to_owned(),
                        score: 80,
                    },
                    ScoredPresentationEntry {
                        wiki_id: ids::WikiId::new(2),
                        name: "name2".to_owned(),
                        score: 20,
                    },
                ],
            },
        };

        let original_string = r#"{"id":"topic","data":{"entries":[{"wiki_id":1,"name":"name1","score":80},{"wiki_id":2,"name":"name2","score":20}]}}"#.to_owned();

        let serialized_string = serde_json::to_string(&original_presentation).unwrap();
        assert_eq!(serialized_string, original_string);

        let deserialized_presentation: Presentation =
            serde_json::from_str(&original_string).unwrap();
        assert_eq!(deserialized_presentation, original_presentation);
    }

    #[test]
    fn serde_presentation_with_scored_data_postcard() {
        let original_presentation = Presentation {
            id: "topic".to_owned(),
            data: PresentationData {
                entries: vec![
                    ScoredPresentationEntry {
                        wiki_id: ids::WikiId::new(1),
                        name: "name1".to_owned(),
                        score: 80,
                    },
                    ScoredPresentationEntry {
                        wiki_id: ids::WikiId::new(2),
                        name: "name2".to_owned(),
                        score: 20,
                    },
                ],
            },
        };

        let serialized_presentation = postcard::to_stdvec(&original_presentation).unwrap();
        let deserialized_presentation: Presentation =
            postcard::from_bytes(&serialized_presentation).unwrap();
        assert_eq!(deserialized_presentation, original_presentation);
    }
}
