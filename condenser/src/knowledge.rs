//! This modules contains definitions of data stored in the internal database.

use std::collections::HashSet;

use merge::Merge;
use serde::{Deserialize, Serialize};

pub use sustainity_collecting::{
    data::{Gtin, OrganisationId, ProductId, VatId, WikiId, WikiStrId},
    sustainity,
};

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
    pub fn new_wikidata(text: String) -> Self {
        Self { text, source: Source::Wikidata }
    }

    /// Constructs a new `Text` with "Open Food Facts" as the source.
    pub fn new_open_food_facts(text: String) -> Self {
        Self { text, source: Source::OpenFoodFacts }
    }

    /// Constructs a new `Text` with "Eu Ecolabel" as the source.
    pub fn new_eu_ecolabel(text: String) -> Self {
        Self { text, source: Source::EuEcolabel }
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
    pub fn new_wikidata(image: String) -> Self {
        Self { image, source: Source::Wikidata }
    }

    /// Constructs a new `Text` with "Open Food Facts" as the source.
    pub fn new_open_food_facts(image: String) -> Self {
        Self { image, source: Source::OpenFoodFacts }
    }
}

/// Data about a `BCorp` company.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BCorpCert {
    /// Name identifying the company.
    pub id: String,
}

/// Data about a company ccertified by EU Ecolabel.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EuEcolabelCert {
    /// Accuracy of match between the comapny name and matched Wikidata item labels.
    pub match_accuracy: f64,
}

/// Data about a company scored by Fashion Transparency Index.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FtiCert {
    /// Score (from 0% to 100%).
    pub score: usize,
}

/// Data about a company which products were certified by TCO.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TcoCert {
    /// Name identifying the company.
    pub brand_name: String,
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
    pub fn new_with_eu_ecolabel(match_accuracy: f64) -> Self {
        Self {
            bcorp: None,
            eu_ecolabel: Some(EuEcolabelCert { match_accuracy }),
            tco: None,
            fti: None,
        }
    }

    /// Copies certifications.
    ///
    /// Eu Ecolabel is not inherited - this certification is assigned directly to products, not companies.
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
pub struct Organisation {
    /// DB entry ID.
    #[serde(rename = "_id")]
    pub db_id: String,

    /// Organisation ID.
    #[serde(rename = "id")]
    pub id: OrganisationId,

    /// VAT IDs.
    #[serde(rename = "vat_ids")]
    pub vat_ids: HashSet<VatId>,

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

impl merge::Merge for Organisation {
    fn merge(&mut self, other: Self) {
        if self.id != other.id {
            return;
        }
        self.vat_ids.extend(other.vat_ids.into_iter());
        self.names.extend_from_slice(&other.names);
        self.descriptions.extend_from_slice(&other.descriptions);
        self.images.extend_from_slice(&other.images);
        self.websites.extend(other.websites.into_iter());
        self.certifications.merge(other.certifications);
    }
}

/// Represents a product.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Product {
    /// DB entry ID.
    #[serde(rename = "_id")]
    pub db_id: String,

    /// Product ID.
    #[serde(rename = "id")]
    pub id: ProductId,

    /// GTIN or the product.
    #[serde(rename = "gtins")]
    pub gtins: HashSet<Gtin>,

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
    pub follows: HashSet<ProductId>,

    /// Wikidata IDs older version products.
    #[serde(rename = "followed_by")]
    pub followed_by: HashSet<ProductId>,
}

impl merge::Merge for Product {
    fn merge(&mut self, other: Self) {
        if self.id != other.id {
            return;
        }
        self.gtins.extend(other.gtins.into_iter());
        self.names.extend_from_slice(&other.names);
        self.descriptions.extend_from_slice(&other.descriptions);
        self.images.extend_from_slice(&other.images);
        self.certifications.merge(other.certifications);
        self.follows.extend(other.follows.into_iter());
        self.followed_by.extend(other.followed_by.into_iter());
    }
}

/// One enttry in `PresentationData::Scored`.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ScoredPresentationEntry {
    /// Organisation ID.
    #[serde(rename = "id")]
    pub id: OrganisationId,

    /// Name of the organisation (as originally listed by the certifier).
    #[serde(rename = "name")]
    pub name: String,

    /// Score from the certifier.
    #[serde(rename = "score")]
    pub score: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum PresentationData {
    Scored(Vec<ScoredPresentationEntry>),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Presentation {
    /// Topic ID.
    pub id: sustainity::data::LibraryTopic,

    /// Data to be presented.
    pub data: PresentationData,
}

/// Represents a topic info.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LibraryInfo {
    /// Topic ID.
    pub id: sustainity::data::LibraryTopic,

    /// Article title.
    pub title: String,

    /// Short one line summary of the article.
    pub summary: String,

    /// Contents of the article in markdown format.
    pub article: String,
}
