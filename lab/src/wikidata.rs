// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Extensions for types from `transpaer_wikidata` crate.

use std::collections::HashSet;

use transpaer_models::utils;
use transpaer_wikidata::{data, errors, properties};

pub use transpaer_wikidata::data::Id as WikiId;

pub mod organisations {
    pub const BUSSINESS: u64 = 4_830_453;
    pub const PUBLIC_COMPANY: u64 = 891_723;
    pub const BRAND: u64 = 431_289;
    pub const RETAIL_CHAIN: u64 = 507_619;
    pub const FASHION_HOUSE: u64 = 3_661_311;
    pub const ENTERPRISE: u64 = 6_881_511;
    pub const ONLINE_SHOP: u64 = 4_382_945;
    pub const SUPERMARKET_CHAIN: u64 = 18_043_413;
    pub const CONCERN: u64 = 206_361;
    pub const CONSUMER_COOPERATIVE: u64 = 614_084;
    pub const BRICK_AND_MORTAR: u64 = 726_870;
    pub const COMPANY: u64 = 783_794;
    pub const SUBSIDIARY: u64 = 658_255;
    pub const DEPARTMENT_STORE_CHAIN: u64 = 2_549_179;
    pub const DEPARTMENT_STORE: u64 = 216_107;
    pub const FOOD_MANUFACTURER: u64 = 1_252_971;

    pub const ALL: &[u64] = &[
        BUSSINESS,
        PUBLIC_COMPANY,
        BRAND,
        RETAIL_CHAIN,
        FASHION_HOUSE,
        ENTERPRISE,
        ONLINE_SHOP,
        SUPERMARKET_CHAIN,
        CONCERN,
        CONSUMER_COOPERATIVE,
        BRICK_AND_MORTAR,
        COMPANY,
        SUBSIDIARY,
        DEPARTMENT_STORE_CHAIN,
        DEPARTMENT_STORE,
        FOOD_MANUFACTURER,
    ];
}

#[allow(dead_code)]
pub trait ItemExt {
    /// Returns items label in the speified language.
    fn get_label(&self, lang: data::Language) -> Option<&str>;

    /// Returns all labels proritizing English.
    fn get_labels(&self) -> Vec<&str>;

    /// Returns all labels and aliases.
    fn get_all_labels_and_aliases(&self) -> HashSet<&str>;

    /// Returns ID associated with the passed property.
    fn get_entity_ids(
        &self,
        property_id: &str,
    ) -> Result<Option<Vec<data::Id>>, errors::ParseIdError>;

    /// Returns strings associated with the passed property.
    fn get_strings(&self, property_id: &str) -> Option<Vec<String>>;

    /// Checks if has at least one passed property.
    fn has_property(&self, property_id: &str) -> bool;

    /// Checks if this item is related to another via a specified property.
    fn relates(&self, property_id: &str, class_id: &str) -> bool;

    // Returns IDs of entries linked with "country" property.
    fn get_countries(&self) -> Result<Option<Vec<data::Id>>, errors::ParseIdError>;

    /// Returns IDs of entities linked with "follows" property.
    fn get_follows(&self) -> Result<Option<Vec<data::Id>>, errors::ParseIdError>;

    /// Returns IDs of entities linked with "followed by" property.
    fn get_followed_by(&self) -> Result<Option<Vec<data::Id>>, errors::ParseIdError>;

    /// Returns IDs of entities linked with "manufacturer" property.
    fn get_manufacturer_ids(&self) -> Result<Option<Vec<data::Id>>, errors::ParseIdError>;

    /// Checks if has any entities linked with "manufacturer" property.
    #[must_use]
    fn has_manufacturer(&self) -> bool;

    /// Returns IDs of entities linked with "product" property.
    fn get_product_ids(&self) -> Result<Option<Vec<data::Id>>, errors::ParseIdError>;

    /// Checks if has any entities linked with "product" property.
    #[must_use]
    fn has_products(&self) -> bool;

    /// Returns IDs of entities linked with "official website" property.
    #[must_use]
    fn get_official_websites(&self) -> Option<Vec<String>>;

    /// Checks if has entities linked with "official website" property.
    #[must_use]
    fn has_official_website(&self) -> bool;

    /// Returns strings associated with the "image" property.
    #[must_use]
    fn get_images(&self) -> Option<Vec<String>>;

    /// Checks if has associated images.
    #[must_use]
    fn has_image(&self) -> bool;

    /// Returns strings associated with the "logo image" property.
    #[must_use]
    fn get_logo_images(&self) -> Option<Vec<String>>;

    /// Checks if has associated logo images.
    #[must_use]
    fn has_logo_image(&self) -> bool;

    /// Checks if this items is linked to the passed entity with `instalce of` property.
    ///
    /// In simpler words: chacks if this item is an instance of the passed class.
    #[must_use]
    fn is_instance_of(&self, class: &str) -> bool;

    /// Returns IDs of classes this item is an instance of.
    fn get_classes(&self) -> Result<Option<Vec<data::Id>>, errors::ParseIdError>;

    /// Checks if this item is a subclass of the given class.
    #[must_use]
    fn is_subclass_of(&self, class: &str) -> bool;

    /// Returns all superclasses of this item.
    fn get_superclasses(&self) -> Result<Option<Vec<data::Id>>, errors::ParseIdError>;

    /// Returns strings associated with the "GTIN" property.
    #[must_use]
    fn get_gtins(&self) -> Option<Vec<String>>;

    /// Checks if has associated "GTIN" values.
    #[must_use]
    fn has_gtin(&self) -> bool;

    /// Returns strings associated with the "ASIN" property.
    #[must_use]
    fn get_asins(&self) -> Option<Vec<String>>;

    /// Checks if has associated "ASIN" values.
    #[must_use]
    fn has_asin(&self) -> bool;

    /// Returns strings associated with the "EU VAT" property.
    #[must_use]
    fn get_eu_vat_numbers(&self) -> Option<Vec<String>>;

    /// Checks if has associated "EU VAT" values.
    #[must_use]
    fn has_eu_vat_number(&self) -> bool;

    /// Checks if this item can be clasified as an organisation.
    #[must_use]
    fn is_organisation(&self) -> bool;

    /// Checks if this item can be clasified as a product.
    #[must_use]
    fn is_product(&self) -> bool;

    /// Extracts internet domains from website addresses.
    #[must_use]
    fn extract_domains(&self) -> Option<HashSet<String>>;
}

impl ItemExt for data::Item {
    fn get_label(&self, lang: data::Language) -> Option<&str> {
        self.labels.get(lang.as_str()).map(|label| label.value.as_str())
    }

    fn get_labels(&self) -> Vec<&str> {
        if let Some(en_label) = self.labels.get(data::Language::En.as_str()) {
            vec![en_label.value.as_str()]
        } else {
            let mut labels = Vec::new();
            for (lang, label) in &self.labels {
                if lang != data::Language::En.as_str() {
                    labels.push(label.value.as_str());
                }
            }
            labels
        }
    }

    fn get_all_labels_and_aliases(&self) -> HashSet<&str> {
        let mut result = HashSet::new();
        for label in self.labels.values() {
            result.insert(label.value.as_str());
        }
        for aliases in self.aliases.values() {
            for alias in aliases {
                result.insert(alias.value.as_str());
            }
        }
        result
    }

    fn get_entity_ids(
        &self,
        property_id: &str,
    ) -> Result<Option<Vec<data::Id>>, errors::ParseIdError> {
        if let Some(claims) = self.claims.get(property_id) {
            let mut result = Vec::<data::Id>::new();
            for claim in claims {
                let data::Claim::Statement(statement) = claim;
                if let data::Snak::Value(value) = &statement.mainsnak {
                    if let data::DataValue::WikibaseEntityId(data::EntityIdDataValue::Item(
                        entity_info,
                    )) = &value.datavalue
                    {
                        result.push(entity_info.id.to_num_id()?);
                    }
                }
            }
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }

    fn relates(&self, property: &str, class: &str) -> bool {
        if let Some(claims) = self.claims.get(property) {
            for claim in claims {
                let data::Claim::Statement(statement) = claim;
                if let data::Snak::Value(value) = &statement.mainsnak {
                    if let data::DataValue::WikibaseEntityId(data::EntityIdDataValue::Item(
                        entity_info,
                    )) = &value.datavalue
                    {
                        if entity_info.id == class {
                            return true;
                        }
                    }
                }
            }
        }
        false
    }

    fn get_strings(&self, property_id: &str) -> Option<Vec<String>> {
        if let Some(claims) = self.claims.get(property_id) {
            let mut result = Vec::new();
            for claim in claims {
                let data::Claim::Statement(statement) = claim;
                if let data::Snak::Value(value) = &statement.mainsnak {
                    if let data::DataValue::String(website) = &value.datavalue {
                        result.push(website.clone());
                    } else {
                        log::warn!(
                            "Item {:?} has properties {} which are not strings: {:?}",
                            self.id,
                            property_id,
                            value
                        );
                    }
                }
            }
            Some(result)
        } else {
            None
        }
    }

    fn has_property(&self, property_id: &str) -> bool {
        if let Some(claims) = self.claims.get(property_id) { !claims.is_empty() } else { false }
    }

    fn get_countries(&self) -> Result<Option<Vec<data::Id>>, errors::ParseIdError> {
        self.get_entity_ids(properties::COUNTRY)
    }

    fn get_follows(&self) -> Result<Option<Vec<data::Id>>, errors::ParseIdError> {
        self.get_entity_ids(properties::FOLLOWS)
    }

    fn get_followed_by(&self) -> Result<Option<Vec<data::Id>>, errors::ParseIdError> {
        self.get_entity_ids(properties::FOLLOWED_BY)
    }

    fn get_manufacturer_ids(&self) -> Result<Option<Vec<data::Id>>, errors::ParseIdError> {
        self.get_entity_ids(properties::MANUFACTURER)
    }

    fn has_manufacturer(&self) -> bool {
        self.has_property(properties::MANUFACTURER)
    }

    fn get_product_ids(&self) -> Result<Option<Vec<data::Id>>, errors::ParseIdError> {
        self.get_entity_ids(properties::PRODUCT_MATERIAL_OR_SERVICE)
    }

    fn has_products(&self) -> bool {
        self.has_property(properties::PRODUCT_MATERIAL_OR_SERVICE)
    }

    fn get_official_websites(&self) -> Option<Vec<String>> {
        self.get_strings(properties::OFFICIAL_WEBSITE)
    }

    fn has_official_website(&self) -> bool {
        self.has_property(properties::OFFICIAL_WEBSITE)
    }

    fn get_images(&self) -> Option<Vec<String>> {
        self.get_strings(properties::IMAGE)
    }

    fn has_image(&self) -> bool {
        self.has_property(properties::IMAGE)
    }

    fn get_logo_images(&self) -> Option<Vec<String>> {
        self.get_strings(properties::LOGO_IMAGE)
    }

    fn has_logo_image(&self) -> bool {
        self.has_property(properties::LOGO_IMAGE)
    }

    fn is_instance_of(&self, class: &str) -> bool {
        self.relates(properties::INSTANCE_OF, class)
    }

    fn get_classes(&self) -> Result<Option<Vec<data::Id>>, errors::ParseIdError> {
        self.get_entity_ids(properties::INSTANCE_OF)
    }

    fn is_subclass_of(&self, class: &str) -> bool {
        self.relates(properties::SUBCLASS_OF, class)
    }

    fn get_superclasses(&self) -> Result<Option<Vec<data::Id>>, errors::ParseIdError> {
        self.get_entity_ids(properties::SUBCLASS_OF)
    }

    fn get_gtins(&self) -> Option<Vec<String>> {
        self.get_strings(properties::GTIN)
    }

    fn has_gtin(&self) -> bool {
        self.has_property(properties::GTIN)
    }

    fn get_asins(&self) -> Option<Vec<String>> {
        self.get_strings(properties::ASIN)
    }

    fn has_asin(&self) -> bool {
        self.has_property(properties::ASIN)
    }

    fn get_eu_vat_numbers(&self) -> Option<Vec<String>> {
        self.get_strings(properties::EU_VAT_NUMBER)
    }

    fn has_eu_vat_number(&self) -> bool {
        self.has_property(properties::EU_VAT_NUMBER)
    }

    fn is_organisation(&self) -> bool {
        if self.has_eu_vat_number() {
            return true;
        }

        if self.has_products() {
            return true;
        }

        if self.has_manufacturer() {
            return false;
        }

        if let Ok(Some(ids)) = self.get_classes() {
            for id in ids {
                if organisations::ALL.contains(&id.get_value()) {
                    return true;
                }
            }
        }

        false
    }

    fn is_product(&self) -> bool {
        self.has_manufacturer() || self.has_gtin()
    }

    fn extract_domains(&self) -> Option<HashSet<String>> {
        self.get_official_websites().map(|u| utils::extract_domains_from_urls(&u))
    }
}
