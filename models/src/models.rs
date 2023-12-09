//! This modules contains definitions of data stored in the internal database.

use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

use merge::Merge;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;

#[cfg(feature = "into-api")]
use sustainity_api::models as api;

use crate::ids;

pub type LibraryTopic = String;
pub type ReadGtin = usize;
pub type ReadProductId = String;
pub type ReadOrganisationId = String;
pub type ReadVatId = String;

#[cfg(feature = "into-api")]
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum IntoApiError {
    #[snafu(display("Failed conversion to LibraryTopic"))]
    ToLibraryTopic { message: String },
}

#[cfg(feature = "into-api")]
impl IntoApiError {
    pub fn to_library_topic(message: String) -> Self {
        Self::ToLibraryTopic { message }
    }
}

/// Points to a source of some data.
#[derive(Serialize, Deserialize, Debug, Clone)]
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
}

#[cfg(feature = "into-api")]
impl Source {
    pub fn into_api(self) -> api::DataSource {
        match self {
            Self::EuEcolabel => api::DataSource::Eu,
            Self::OpenFoodFacts => api::DataSource::Off,
            Self::Wikidata => api::DataSource::Wiki,
        }
    }
}

/// Text together with it's source.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Text {
    /// Text.
    #[serde(rename = "text")]
    pub text: String,

    /// Source of the text.
    #[serde(rename = "source")]
    pub source: Source,
}

impl Text {
    /// Constructs a new `Text` with "Wikidata" as the source.
    #[must_use]
    pub fn new_wikidata(text: String) -> Self {
        Self { text, source: Source::Wikidata }
    }

    /// Constructs a new `Text` with "Open Food Facts" as the source.
    #[must_use]
    pub fn new_open_food_facts(text: String) -> Self {
        Self { text, source: Source::OpenFoodFacts }
    }

    /// Constructs a new `Text` with "Eu Ecolabel" as the source.
    #[must_use]
    pub fn new_eu_ecolabel(text: String) -> Self {
        Self { text, source: Source::EuEcolabel }
    }
}

#[cfg(feature = "into-api")]
impl Text {
    pub fn into_api_long(self) -> api::LongText {
        api::LongText { text: self.text, source: self.source.into_api() }
    }

    pub fn into_api_short(self) -> api::ShortText {
        api::ShortText { text: self.text, source: self.source.into_api() }
    }
}

/// Image together with it's source.
#[derive(Serialize, Deserialize, Debug, Clone)]
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

impl Image {
    /// Constructs a new `Text` with "Wikidata" as the source.
    #[must_use]
    pub fn new_wikidata(image: String) -> Self {
        Self { image, source: Source::Wikidata }
    }

    /// Constructs a new `Text` with "Open Food Facts" as the source.
    #[must_use]
    pub fn new_open_food_facts(image: String) -> Self {
        Self { image, source: Source::OpenFoodFacts }
    }
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
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BCorpCert {
    /// Name identifying the company.
    pub id: String,
}

#[cfg(feature = "into-api")]
impl BCorpCert {
    pub fn into_api(self) -> api::Medallion {
        api::Medallion {
            variant: api::MedallionVariant::BCorp,
            bcorp: Some(api::BCorpMedallion { id: self.id }),
            eu_ecolabel: None,
            fti: None,
            sustainity: None,
            tco: None,
        }
    }
}

/// Data about a company ccertified by EU Ecolabel.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EuEcolabelCert {
    /// Accuracy of match between the comapny name and matched Wikidata item labels.
    pub match_accuracy: f64,
}

#[cfg(feature = "into-api")]
impl EuEcolabelCert {
    pub fn into_api(self) -> api::Medallion {
        api::Medallion {
            variant: api::MedallionVariant::EuEcolabel,
            bcorp: None,
            eu_ecolabel: Some(api::EuEcolabelMedallion { match_accuracy: self.match_accuracy }),
            fti: None,
            sustainity: None,
            tco: None,
        }
    }
}

/// Data about a company scored by Fashion Transparency Index.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FtiCert {
    /// Score (from 0% to 100%).
    pub score: i32,
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
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TcoCert {
    /// Name identifying the company.
    pub brand_name: String,
}

#[cfg(feature = "into-api")]
impl TcoCert {
    pub fn into_api(self) -> api::Medallion {
        api::Medallion {
            variant: api::MedallionVariant::Tco,
            bcorp: None,
            eu_ecolabel: None,
            fti: None,
            sustainity: None,
            tco: Some(api::TcoMedallion { brand_name: self.brand_name }),
        }
    }
}

/// Lists known certifications.
#[derive(Serialize, Deserialize, Debug, Clone, Default, Merge)]
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
    /// Constructs a new `Certifications` with only `eu_ecolabel` set.
    #[must_use]
    pub fn new_with_eu_ecolabel(match_accuracy: f64) -> Self {
        Self {
            bcorp: None,
            eu_ecolabel: Some(EuEcolabelCert { match_accuracy }),
            tco: None,
            fti: None,
        }
    }

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
            self.bcorp = other.bcorp.clone();
        }
        if other.fti.is_some() {
            self.fti = other.fti.clone();
        }
        if other.tco.is_some() {
            self.tco = other.tco.clone();
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

    pub fn to_api_scores(&self) -> HashMap<String, i32> {
        let mut scores = HashMap::new();
        if let Some(fti) = &self.fti {
            scores.insert(api::ScorerName::Fti.to_string(), fti.score);
        }
        scores
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct IdEntry {
    /// DB entry ID.
    #[serde(rename = "_id")]
    pub db_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct Keyword {
    /// DB entry ID.
    #[serde(rename = "_id")]
    pub db_id: String,

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
    pub weight: u32,

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
            weight: self.weight as i32,
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

/// Represents an organisation (e.g. manufacturer, shop).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Organisation<O, V>
where
    V: std::hash::Hash + std::cmp::Eq,
{
    /// DB entry ID.
    #[serde(rename = "_id")]
    pub db_id: String,

    /// Organisation ID.
    #[serde(rename = "id")]
    pub id: O,

    /// VAT IDs.
    #[serde(rename = "vat_ids")]
    pub vat_ids: HashSet<V>,

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
    pub websites: HashSet<String>,

    /// Known certifications.
    #[serde(rename = "certifications")]
    pub certifications: Certifications,
}

pub type ReadOrganisation = Organisation<String, String>;
pub type WriteOrganisation = Organisation<ids::OrganisationId, ids::VatId>;

impl merge::Merge for WriteOrganisation {
    fn merge(&mut self, other: Self) {
        if self.id != other.id {
            return;
        }
        self.vat_ids.extend(other.vat_ids);
        self.names.extend_from_slice(&other.names);
        self.descriptions.extend_from_slice(&other.descriptions);
        self.images.extend_from_slice(&other.images);
        self.websites.extend(other.websites);
        self.certifications.merge(other.certifications);
    }
}

#[cfg(feature = "into-api")]
impl ReadOrganisation {
    pub fn into_api_short(self) -> api::OrganisationShort {
        api::OrganisationShort {
            organisation_id: self.id,
            name: self.names.first().map_or_else(String::default, |n| n.text.clone()),
            description: self.descriptions.first().map(|d| d.text.clone()),
            badges: self.certifications.to_api_badges(),
            scores: self.certifications.to_api_scores(),
        }
    }

    pub fn into_api_full(self, products: Vec<api::ProductShort>) -> api::OrganisationFull {
        api::OrganisationFull {
            organisation_id: self.id,
            names: Some(self.names.into_iter().map(|n| n.into_api_short()).collect()),
            descriptions: Some(self.descriptions.into_iter().map(|d| d.into_api_long()).collect()),
            images: self.images.into_iter().map(|i| i.into_api()).collect(),
            websites: self.websites.into_iter().map(From::from).collect(),
            medallions: self.certifications.into_api_medallions(),
            products,
        }
    }
}

/// Represents a product.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Product<P, G>
where
    P: std::hash::Hash + std::cmp::Eq,
    G: std::hash::Hash + std::cmp::Eq,
{
    /// DB entry ID.
    #[serde(rename = "_id")]
    pub db_id: String,

    /// Product ID.
    #[serde(rename = "id")]
    pub id: P,

    /// GTIN or the product.
    #[serde(rename = "gtins")]
    pub gtins: HashSet<G>,

    /// Names of the product.
    #[serde(rename = "names")]
    pub names: Vec<Text>,

    /// Descriptions of the product.
    #[serde(rename = "descriptions")]
    pub descriptions: Vec<Text>,

    /// Product images.
    #[serde(rename = "images")]
    pub images: Vec<Image>,

    /// Known certifications.
    #[serde(rename = "certifications")]
    pub certifications: Certifications,

    /// Wikidata IDs newer version products.
    #[serde(rename = "follows")]
    pub follows: HashSet<P>,

    /// Wikidata IDs older version products.
    #[serde(rename = "followed_by")]
    pub followed_by: HashSet<P>,

    /// Regions where the product is available.
    #[serde(rename = "regions")]
    pub regions: Regions,

    /// The Sustainity score.
    #[serde(rename = "sustainity_score")]
    pub sustainity_score: SustainityScore,
}

pub type ReadProduct = Product<ReadProductId, ReadGtin>;
pub type WriteProduct = Product<ids::ProductId, ids::Gtin>;

impl merge::Merge for WriteProduct {
    fn merge(&mut self, other: Self) {
        if self.id != other.id {
            return;
        }
        self.gtins.extend(other.gtins);
        self.names.extend_from_slice(&other.names);
        self.descriptions.extend_from_slice(&other.descriptions);
        self.images.extend_from_slice(&other.images);
        self.certifications.merge(other.certifications);
        self.follows.extend(other.follows);
        self.followed_by.extend(other.followed_by);
    }
}

#[cfg(feature = "into-api")]
impl ReadProduct {
    pub fn into_api_short(self) -> api::ProductShort {
        api::ProductShort {
            product_id: self.id,
            name: self.names.first().map_or_else(String::default, |n| n.text.clone()),
            description: self.descriptions.first().map(|d| d.text.clone()),
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
            product_id: self.id,
            gtins: Some(self.gtins.into_iter().map(|gtin| gtin.to_string().into()).collect()),
            names: Some(self.names.into_iter().map(|n| n.into_api_short()).collect()),
            descriptions: Some(self.descriptions.into_iter().map(|d| d.into_api_long()).collect()),
            images: self.images.into_iter().map(|i| i.into_api()).collect(),
            manufacturers,
            alternatives,
            medallions,
        }
    }
}

/// One enttry in `PresentationData::Scored`.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ScoredPresentationEntry<O> {
    /// Organisation ID.
    #[serde(rename = "id")]
    pub id: O,

    /// Name of the organisation (as originally listed by the certifier).
    #[serde(rename = "name")]
    pub name: String,

    /// Score from the certifier.
    #[serde(rename = "score")]
    pub score: i32,
}

pub type ReadScoredPresentationEntry = ScoredPresentationEntry<ReadOrganisationId>;
pub type WriteScoredPresentationEntry = ScoredPresentationEntry<ids::OrganisationId>;

#[cfg(feature = "into-api")]
impl ReadScoredPresentationEntry {
    pub fn into_api(self) -> api::PresentationEntry {
        api::PresentationEntry { id: self.id, name: self.name, score: self.score }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum PresentationData<O> {
    Scored(Vec<ScoredPresentationEntry<O>>),
}

pub type ReadPresentationData = PresentationData<ReadOrganisationId>;
pub type WritePresentationData = PresentationData<ids::OrganisationId>;

#[cfg(feature = "into-api")]
impl ReadPresentationData {
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

pub type ReadPresentation = Presentation<ReadOrganisationId>;
pub type WritePresentation = Presentation<ids::OrganisationId>;

#[cfg(feature = "into-api")]
impl ReadPresentation {
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
            title: self.title,
            summary: self.summary,
        })
    }

    pub fn try_into_api_full(
        self,
        presentation: Option<api::Presentation>,
    ) -> Result<api::LibraryItemFull, IntoApiError> {
        Ok(api::LibraryItemFull {
            id: api::LibraryTopic::from_str(&self.id).map_err(IntoApiError::to_library_topic)?,
            title: self.title,
            summary: self.summary,
            article: self.article,
            presentation,
        })
    }
}
