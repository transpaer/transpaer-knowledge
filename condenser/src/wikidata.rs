//! Extensions for types from `consumers_wikidata` crate.

use consumers_wikidata::{data, properties};

pub trait ItemExt {
    /// Returns ID associated with the passed property.
    fn get_entity_ids(&self, property_id: &str) -> Option<Vec<data::Id>>;

    /// Returns strings associated with the passed property.
    fn get_strings(&self, property_id: &str) -> Option<Vec<String>>;

    /// Checks if has at least one passed property.
    fn has_property(&self, property_id: &str) -> bool;

    /// Returns IDs of entities linked with "follows" property.
    #[must_use]
    fn get_follows(&self) -> Option<Vec<data::Id>>;

    /// Returns IDs of entities linked with "followed by" property.
    #[must_use]
    fn get_followed_by(&self) -> Option<Vec<data::Id>>;

    /// Returns IDs of entities linked with "manufacturer" property.
    #[must_use]
    fn get_manufacturer_ids(&self) -> Option<Vec<data::Id>>;

    /// Checks if has any entities linked with "manufacturer" property.
    #[must_use]
    fn has_manufacturer(&self) -> bool;

    /// Returns IDs of entities linked with "official website" property.
    #[must_use]
    fn get_official_websites(&self) -> Option<Vec<String>>;

    /// Checks if has entities linked with "official website" property.
    #[must_use]
    fn has_official_website(&self) -> bool;

    /// Checks if this items is linked to the passed entity with `instalce of` property.
    ///
    /// In simpler words: chacks if this item is an instance of the passed class.
    #[must_use]
    fn is_instance_of(&self, class: &str) -> bool;
}

impl ItemExt for data::Item {
    /// Returns ID associated with the passed property.
    fn get_entity_ids(&self, property_id: &str) -> Option<Vec<data::Id>> {
        if let Some(claims) = self.claims.get(property_id) {
            let mut result = Vec::<data::Id>::new();
            for claim in claims {
                let data::Claim::Statement(statement) = claim;
                if let data::Snak::Value(value) = &statement.mainsnak {
                    if let data::DataValue::WikibaseEntityId(data::EntityIdDataValue::Item(
                        entity_info,
                    )) = &value.datavalue
                    {
                        result.push(entity_info.id.clone());
                    }
                }
            }
            Some(result)
        } else {
            None
        }
    }

    /// Returns strings associated with the passed property.
    fn get_strings(&self, property_id: &str) -> Option<Vec<String>> {
        if let Some(claims) = self.claims.get(property_id) {
            let mut result = Vec::new();
            for claim in claims {
                let data::Claim::Statement(statement) = claim;
                if let data::Snak::Value(value) = &statement.mainsnak {
                    if let data::DataValue::String(website) = &value.datavalue {
                        result.push(website.clone());
                    }
                }
            }
            Some(result)
        } else {
            None
        }
    }

    /// Checks if has at least one passed property.
    fn has_property(&self, property_id: &str) -> bool {
        if let Some(claims) = self.claims.get(property_id) {
            !claims.is_empty()
        } else {
            false
        }
    }

    /// Returns IDs of entities linked with "follows" property.
    #[must_use]
    fn get_follows(&self) -> Option<Vec<data::Id>> {
        self.get_entity_ids(properties::FOLLOWS)
    }

    /// Returns IDs of entities linked with "followed by" property.
    #[must_use]
    fn get_followed_by(&self) -> Option<Vec<data::Id>> {
        self.get_entity_ids(properties::FOLLOWED_BY)
    }

    /// Returns IDs of entities linked with "manufacturer" property.
    #[must_use]
    fn get_manufacturer_ids(&self) -> Option<Vec<data::Id>> {
        self.get_entity_ids(properties::MANUFACTURER)
    }

    /// Checks if has any entities linked with "manufacturer" property.
    #[must_use]
    fn has_manufacturer(&self) -> bool {
        self.has_property(properties::MANUFACTURER)
    }

    /// Returns IDs of entities linked with "official website" property.
    #[must_use]
    fn get_official_websites(&self) -> Option<Vec<String>> {
        self.get_strings(properties::OFFICIAL_WEBSITE)
    }

    /// Checks if has entities linked with "official website" property.
    #[must_use]
    fn has_official_website(&self) -> bool {
        self.has_property(properties::OFFICIAL_WEBSITE)
    }

    /// Checks if this items is linked to the passed entity with `instalce of` property.
    ///
    /// In simpler words: chacks if this item is an instance of the passed class.
    #[must_use]
    fn is_instance_of(&self, class: &str) -> bool {
        if let Some(claims) = self.claims.get(properties::INSTANCE_OF) {
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
}
