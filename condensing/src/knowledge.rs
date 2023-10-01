//! This modules contains definitions of data stored in the internal database.

use std::collections::HashSet;

use merge::Merge;
use serde::{Deserialize, Serialize};

use sustainity_collecting::open_food_facts;
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

impl TryFrom<&open_food_facts::data::Regions> for Regions {
    type Error = isocountry::CountryCodeParseErr;

    fn try_from(regions: &open_food_facts::data::Regions) -> Result<Self, Self::Error> {
        Ok(match regions {
            open_food_facts::data::Regions::World => Self::World,
            open_food_facts::data::Regions::Unknown => Self::Unknown,
            open_food_facts::data::Regions::List(list) => {
                let regions = list
                    .iter()
                    .map(|c| isocountry::CountryCode::for_alpha2(c))
                    .collect::<Result<Vec<isocountry::CountryCode>, Self::Error>>()?;
                Self::List(regions)
            }
        })
    }
}

impl From<&Regions> for open_food_facts::data::Regions {
    fn from(regions: &Regions) -> open_food_facts::data::Regions {
        match regions {
            Regions::World => Self::World,
            Regions::Unknown => Self::Unknown,
            Regions::List(list) => {
                let codes = list.iter().map(|r| r.alpha2().to_string()).collect();
                Self::List(codes)
            }
        }
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SustainityScoreBranch {
    /// Subbranches of the tree.
    #[serde(rename = "branches")]
    pub branches: Vec<SustainityScoreBranch>,

    /// Unicode symbol representing this branch.
    #[serde(rename = "symbol")]
    pub symbol: char,

    /// Weight of this branch.
    #[serde(rename = "weight")]
    pub weight: u32,

    /// Calculated subscore of this branch.
    #[serde(rename = "score")]
    pub score: f64,
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
        self.vat_ids.extend(other.vat_ids);
        self.names.extend_from_slice(&other.names);
        self.descriptions.extend_from_slice(&other.descriptions);
        self.images.extend_from_slice(&other.images);
        self.websites.extend(other.websites);
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

    /// Regions where the product is available.
    #[serde(rename = "regions")]
    pub regions: Regions,

    /// The Sustainity score.
    #[serde(rename = "sustainity_score")]
    pub sustainity_score: SustainityScore,
}

impl merge::Merge for Product {
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
