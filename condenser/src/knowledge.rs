//! This modules contains definitions of data stored in the internal database.

use merge::Merge;
use serde::{Deserialize, Serialize};

pub use sustainity_collecting::data::{Gtin, OrganisationId, ProductId, VatId, WikiId, WikiStrId};

/// Points to a source of some data.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Source {
    /// Wikidata.
    #[serde(rename = "wikidata")]
    Wikidata,
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

/// Defines a product categories.
#[allow(clippy::struct_excessive_bools)] // TODO: perhaps rework as an array (depends on DB query  performance)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Categories {
    /// Smartphone
    #[serde(rename = "smartphone")]
    pub smartphone: bool,

    /// Smart watch
    #[serde(rename = "smartwatch")]
    pub smartwatch: bool,

    /// Talet
    #[serde(rename = "tablet")]
    pub tablet: bool,

    /// Laptop
    #[serde(rename = "laptop")]
    pub laptop: bool,

    /// Computer
    #[serde(rename = "computer")]
    pub computer: bool,

    /// Game console
    #[serde(rename = "game_console")]
    pub game_console: bool,

    /// Game controller
    #[serde(rename = "game_controller")]
    pub game_controller: bool,

    /// Camera
    #[serde(rename = "camera")]
    pub camera: bool,

    /// Camera lens
    #[serde(rename = "camera_lens")]
    pub camera_lens: bool,

    /// Microprocessor
    #[serde(rename = "microprocessor")]
    pub microprocessor: bool,

    /// Calculator
    #[serde(rename = "calculator")]
    pub calculator: bool,

    /// Musical instrument
    #[serde(rename = "musical_instrument")]
    pub musical_instrument: bool,

    /// Washing mascine
    #[serde(rename = "washing_machine")]
    pub washing_machine: bool,

    /// Car
    #[serde(rename = "car")]
    pub car: bool,

    /// Motorcycle
    #[serde(rename = "motorcycle")]
    pub motorcycle: bool,

    /// Boat
    #[serde(rename = "boat")]
    pub boat: bool,

    /// Drone
    #[serde(rename = "drone")]
    pub drone: bool,

    /// Drink
    #[serde(rename = "drink")]
    pub drink: bool,

    /// Food
    #[serde(rename = "food")]
    pub food: bool,

    /// Toy
    #[serde(rename = "toy")]
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

    pub fn inherit(&mut self, other: &Self) {
        if other.bcorp {
            self.bcorp = true;
        }
        if other.eu_ecolabel {
            self.eu_ecolabel = true;
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

    /// VAT IDs.
    pub vat_ids: Vec<VatId>,

    /// Name of the organisation.
    pub name: String,

    /// Description of the organisation.
    pub description: Option<String>,

    /// Logo images.
    pub images: Vec<Image>,

    /// Websites.
    pub websites: Vec<String>,

    /// Known certifications.
    pub certifications: Certifications,
}

/// Represents a product.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Product {
    /// Product ID.
    pub id: ProductId,

    /// GTIN or the product.
    pub gtins: Vec<Gtin>,

    /// Name of the product.
    pub name: String,

    /// Description of the product.
    pub description: Option<String>,

    /// Product images.
    pub images: Vec<Image>,

    /// Categories of the product.
    pub categories: Categories,

    /// Wikidata IDs of manufacturers.
    pub manufacturer_ids: Option<Vec<OrganisationId>>,

    /// Wikidata IDs newer version products.
    pub follows: Option<Vec<ProductId>>,

    /// Wikidata IDs older version products.
    pub followed_by: Option<Vec<ProductId>>,

    /// Known certifications.
    pub certifications: Certifications,
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
