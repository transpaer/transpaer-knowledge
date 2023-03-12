//! This modules contains definitions of data stored in the internal database.

use merge::Merge;
use serde::{Deserialize, Serialize};

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
}

/// Represents a product.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Product {
    /// Wikidata ID.
    pub id: consumers_wikidata::data::Id,

    /// Name of the product.
    pub name: String,

    /// Description of the product.
    pub description: String,

    /// Category of the product.
    pub category: Option<Category>,

    /// Wikidata IDs of manufacturers.
    pub manufacturer_ids: Option<Vec<consumers_wikidata::data::Id>>,

    /// Wikidata IDs newer version products.
    pub follows: Option<Vec<consumers_wikidata::data::Id>>,

    /// Wikidata IDs older version products.
    pub followed_by: Option<Vec<consumers_wikidata::data::Id>>,

    /// Known certifications.
    pub certifications: Certifications,
}

/// Represents a manufacturer.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Manufacturer {
    /// Wikidata Id.
    pub id: consumers_wikidata::data::Id,

    /// Name of the manufacturer.
    pub name: String,

    /// Description of the manufacturer.
    pub description: String,

    /// Websites.
    pub websites: Vec<String>,

    /// Known certifications.
    pub certifications: Certifications,
}
