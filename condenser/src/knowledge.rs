//! This modules contains definitions of data stored in the internal database.

use merge::Merge;
use serde::{Deserialize, Serialize};

pub use consumers_wikidata::data::Id;

/// Defins a product category.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Category {
    /// Smartphone
    #[serde(rename = "smartphone")]
    Smartphone,
}

/// Lists known certifications.
#[derive(Serialize, Deserialize, Debug, Clone, Default, Merge)]
pub struct Certifications {
    /// Manufacturer certifiad by BCorp.
    #[merge(strategy = merge::bool::overwrite_false)]
    pub bcorp: bool,

    /// Manufacturer certifiad by TCO.
    #[merge(strategy = merge::bool::overwrite_false)]
    pub tco: bool,

    /// Organisation scored by Fashion Transparency Index.
    pub fti: Option<usize>,
}

/// Represents an organisation (e.g. manufacturer, shop).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Organisation {
    /// Wikidata Id.
    pub id: Id,

    /// Name of the organisation.
    pub name: String,

    /// Description of the organisation.
    pub description: String,

    /// Websites.
    pub websites: Vec<String>,

    /// Known certifications.
    pub certifications: Certifications,
}

/// Represents a product.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Product {
    /// Wikidata ID.
    pub id: Id,

    /// Name of the product.
    pub name: String,

    /// Description of the product.
    pub description: String,

    /// Category of the product.
    pub category: Option<Category>,

    /// Wikidata IDs of manufacturers.
    pub manufacturer_ids: Option<Vec<Id>>,

    /// Wikidata IDs newer version products.
    pub follows: Option<Vec<Id>>,

    /// Wikidata IDs older version products.
    pub followed_by: Option<Vec<Id>>,

    /// Known certifications.
    pub certifications: Certifications,
}

/// Represents a topic info.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Info {
    /// Topic ID.
    pub id: String,

    /// Article title.
    pub title: String,

    /// Contents of the article in markdown format.
    pub article: String,
}
