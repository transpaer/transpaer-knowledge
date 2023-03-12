use std::collections::{HashMap, HashSet};

use merge::Merge;

use crate::knowledge::{Manufacturer, Product};

/// Data storage for gathered data.
///
/// Allows merging different instances.
#[derive(Debug, Default)]
pub struct DataCollector {
    /// Found manugacturer ID.
    manufacturer_ids: HashSet<consumers_wikidata::data::Id>,

    /// Found products.
    products: Vec<Product>,

    /// Found manufacturers.
    manufacturers: HashMap<consumers_wikidata::data::Id, Manufacturer>,
}

impl DataCollector {
    /// Creates a new empty `DataCollector`.
    pub fn new() -> Self {
        Self {
            manufacturer_ids: HashSet::new(),
            products: Vec::new(),
            manufacturers: HashMap::new(),
        }
    }

    /// Adds a new manufacturer ID.
    pub fn add_manufacturer_ids(&mut self, ids: &Vec<consumers_wikidata::data::Id>) {
        for id in ids {
            self.manufacturer_ids.insert(id.clone());
        }
    }

    /// Adds a new product.
    pub fn add_product(&mut self, product: Product) {
        self.products.push(product);
    }

    /// Adds a new manufacturer.
    pub fn add_manufacturer(&mut self, manufacturer: Manufacturer) {
        self.manufacturers.insert(manufacturer.id.clone(), manufacturer);
    }

    /// Returns found manufacturer IDs.
    pub fn get_manufacturer_ids(&self) -> &HashSet<consumers_wikidata::data::Id> {
        &self.manufacturer_ids
    }

    /// Post-processes the data:
    /// - add manufacturer certifications for products
    pub fn postprocess(&mut self) {
        for product in &mut self.products {
            if let Some(manufacturer_ids) = &product.manufacturer_ids {
                for manufacturer_id in manufacturer_ids {
                    if let Some(manufacturer) = self.manufacturers.get(manufacturer_id) {
                        product.certifications.merge(manufacturer.certifications.clone());
                    }
                }
            }
        }
    }

    /// Returns all collected producers.
    pub fn get_products(&self) -> &Vec<Product> {
        &self.products
    }

    /// Returns all collected manufacturers.
    pub fn get_manufacturers(&self) -> &HashMap<consumers_wikidata::data::Id, Manufacturer> {
        &self.manufacturers
    }
}

impl merge::Merge for DataCollector {
    fn merge(&mut self, other: Self) {
        self.manufacturer_ids.extend(other.manufacturer_ids);
        self.products.extend_from_slice(&other.products);
        self.manufacturers.extend(other.manufacturers);
    }
}
