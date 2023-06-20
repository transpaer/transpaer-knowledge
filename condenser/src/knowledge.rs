//! This modules contains definitions of data stored in the internal database.

use std::collections::HashSet;

use merge::Merge;
use serde::{Deserialize, Serialize};

pub use sustainity_collecting::data::{Gtin, OrganisationId, ProductId, VatId, WikiId, WikiStrId};

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

/// Defines a product categories.
/// TODO: perhaps rework as an array (depends on DB query  performance)
#[allow(clippy::struct_excessive_bools)]
#[derive(Serialize, Deserialize, Debug, Clone, Merge)]
pub struct Categories {
    /// Smartphone
    #[serde(rename = "smartphone")]
    #[merge(strategy = merge::bool::overwrite_false)]
    pub smartphone: bool,

    /// Smart watch
    #[serde(rename = "smartwatch")]
    #[merge(strategy = merge::bool::overwrite_false)]
    pub smartwatch: bool,

    /// Talet
    #[serde(rename = "tablet")]
    #[merge(strategy = merge::bool::overwrite_false)]
    pub tablet: bool,

    /// Laptop
    #[serde(rename = "laptop")]
    #[merge(strategy = merge::bool::overwrite_false)]
    pub laptop: bool,

    /// Computer
    #[serde(rename = "computer")]
    #[merge(strategy = merge::bool::overwrite_false)]
    pub computer: bool,

    /// Game console
    #[serde(rename = "game_console")]
    #[merge(strategy = merge::bool::overwrite_false)]
    pub game_console: bool,

    /// Game controller
    #[serde(rename = "game_controller")]
    #[merge(strategy = merge::bool::overwrite_false)]
    pub game_controller: bool,

    /// Camera
    #[serde(rename = "camera")]
    #[merge(strategy = merge::bool::overwrite_false)]
    pub camera: bool,

    /// Camera lens
    #[serde(rename = "camera_lens")]
    #[merge(strategy = merge::bool::overwrite_false)]
    pub camera_lens: bool,

    /// Microprocessor
    #[serde(rename = "microprocessor")]
    #[merge(strategy = merge::bool::overwrite_false)]
    pub microprocessor: bool,

    /// Calculator
    #[serde(rename = "calculator")]
    #[merge(strategy = merge::bool::overwrite_false)]
    pub calculator: bool,

    /// Musical instrument
    #[serde(rename = "musical_instrument")]
    #[merge(strategy = merge::bool::overwrite_false)]
    pub musical_instrument: bool,

    /// Washing mascine
    #[serde(rename = "washing_machine")]
    #[merge(strategy = merge::bool::overwrite_false)]
    pub washing_machine: bool,

    /// Car
    #[serde(rename = "car")]
    #[merge(strategy = merge::bool::overwrite_false)]
    pub car: bool,

    /// Motorcycle
    #[serde(rename = "motorcycle")]
    #[merge(strategy = merge::bool::overwrite_false)]
    pub motorcycle: bool,

    /// Boat
    #[serde(rename = "boat")]
    #[merge(strategy = merge::bool::overwrite_false)]
    pub boat: bool,

    /// Drone
    #[serde(rename = "drone")]
    #[merge(strategy = merge::bool::overwrite_false)]
    pub drone: bool,

    /// Drink
    #[serde(rename = "drink")]
    #[merge(strategy = merge::bool::overwrite_false)]
    pub drink: bool,

    /// Food
    #[serde(rename = "food")]
    #[merge(strategy = merge::bool::overwrite_false)]
    pub food: bool,

    /// Toy
    #[serde(rename = "toy")]
    #[merge(strategy = merge::bool::overwrite_false)]
    pub toy: bool,
}

impl Categories {
    pub fn none() -> Self {
        Self {
            smartphone: false,
            smartwatch: false,
            tablet: false,
            laptop: false,
            computer: false,
            game_console: false,
            game_controller: false,
            camera: false,
            camera_lens: false,
            microprocessor: false,
            calculator: false,
            musical_instrument: false,
            washing_machine: false,
            car: false,
            motorcycle: false,
            boat: false,
            drone: false,
            drink: false,
            food: false,
            toy: false,
        }
    }

    pub fn has_category(&self) -> bool {
        self.smartphone
            || self.smartwatch
            || self.tablet
            || self.laptop
            || self.computer
            || self.game_console
            || self.game_controller
            || self.camera
            || self.camera_lens
            || self.microprocessor
            || self.calculator
            || self.musical_instrument
            || self.washing_machine
            || self.car
            || self.motorcycle
            || self.boat
            || self.drone
            || self.drink
            || self.food
            || self.toy
    }
}

/// Lists known certifications.
#[derive(Serialize, Deserialize, Debug, Clone, Default, Merge)]
pub struct Certifications {
    /// Manufacturer certifiad by BCorp.
    #[merge(strategy = merge::bool::overwrite_false)]
    pub bcorp: bool,

    /// Manufacturer certified by EU Ecolabel.
    #[merge(strategy = merge::bool::overwrite_false)]
    pub eu_ecolabel: bool,

    /// Manufacturer certifiad by TCO.
    #[merge(strategy = merge::bool::overwrite_false)]
    pub tco: bool,

    /// Organisation scored by Fashion Transparency Index.
    pub fti: Option<usize>,
}

impl Certifications {
    pub fn new_with_eu_ecolabel() -> Self {
        Self { bcorp: false, eu_ecolabel: true, tco: false, fti: None }
    }

    /// Copies certifications.
    ///
    /// Eu Ecolabel is not inherited - this certification is assigned directly to products, not companies.
    pub fn inherit(&mut self, other: &Self) {
        if other.bcorp {
            self.bcorp = true;
        }
        if other.tco {
            self.tco = true;
        }
        if other.fti.is_some() {
            self.fti = other.fti;
        }
    }
}

/// Represents an organisation (e.g. manufacturer, shop).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Organisation {
    /// Organisation ID.
    pub id: OrganisationId,

    /// Keywords for database text search.
    pub keywords: HashSet<String>,

    /// VAT IDs.
    pub vat_ids: HashSet<VatId>,

    /// Names of the organisation.
    pub names: Vec<Text>,

    /// Descriptions of the organisation.
    pub descriptions: Vec<Text>,

    /// Logo images.
    pub images: Vec<Image>,

    /// Websites.
    pub websites: HashSet<String>,

    /// Known certifications.
    pub certifications: Certifications,
}

impl merge::Merge for Organisation {
    fn merge(&mut self, other: Self) {
        if self.id != other.id {
            return;
        }
        self.vat_ids.extend(other.vat_ids.into_iter());
        self.keywords.extend(other.keywords.into_iter());
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
    /// Product ID.
    pub id: ProductId,

    /// Keywords for database text search.
    pub keywords: HashSet<String>,

    /// GTIN or the product.
    pub gtins: HashSet<Gtin>,

    /// Names of the product.
    pub names: Vec<Text>,

    /// Descriptions of the product.
    pub descriptions: Vec<Text>,

    /// Product images.
    pub images: Vec<Image>,

    /// Categories of the product.
    pub categories: Categories,

    /// Known certifications.
    pub certifications: Certifications,

    /// Wikidata IDs of manufacturers.
    pub manufacturer_ids: HashSet<OrganisationId>,

    /// Wikidata IDs newer version products.
    pub follows: HashSet<ProductId>,

    /// Wikidata IDs older version products.
    pub followed_by: HashSet<ProductId>,
}

impl merge::Merge for Product {
    fn merge(&mut self, other: Self) {
        if self.id != other.id {
            return;
        }
        self.gtins.extend(other.gtins.into_iter());
        self.keywords.extend(other.keywords.into_iter());
        self.names.extend_from_slice(&other.names);
        self.descriptions.extend_from_slice(&other.descriptions);
        self.images.extend_from_slice(&other.images);
        self.categories.merge(other.categories);
        self.certifications.merge(other.certifications);
        self.manufacturer_ids.extend(other.manufacturer_ids.into_iter());
        self.follows.extend(other.follows.into_iter());
        self.followed_by.extend(other.followed_by.into_iter());
    }
}

/// Represents a topic info.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LibraryInfo {
    /// Topic ID.
    pub id: String,

    /// Article title.
    pub title: String,

    /// Contents of the article in markdown format.
    pub article: String,
}
