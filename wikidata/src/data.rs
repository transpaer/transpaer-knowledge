//! Structures and enums found in this module represents data found in the Wikidata dump file.
//!
//! Many of the stuctures and enums are not finished!

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::properties;

/// Represents a Wikidata ID.
///
/// Internally the ID is representad as a `String`.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, Hash, PartialEq)]
pub struct Id(String);

impl Id {
    /// Constructs a new `Id`.
    #[must_use]
    pub fn new(string: String) -> Self {
        Self(string)
    }

    /// Returns a `&str`.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// Returns a `&String`.
    #[must_use]
    pub fn as_string(&self) -> &String {
        &self.0
    }

    /// Consumes this `Id` and returns the underlying `String`.
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl From<String> for Id {
    fn from(string: String) -> Self {
        Self(string)
    }
}

/// Represents a Wikidata label.
#[derive(Serialize, Deserialize, Debug)]
pub struct Label {
    /// Language of the text.
    pub language: String,

    /// Value of the text.
    pub value: String,
}

/// Represents a Wikidata entity ID.
///
/// The ID is a number with "Q" or "P" prefix.
#[derive(Serialize, Deserialize, Debug)]
pub struct EntityIdItemInfo {
    /// Full ID.
    pub id: String,

    /// Number from the ID without the prefix.
    #[serde(rename = "numeric-id")]
    pub numeric_id: usize,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "entity-type")]
pub enum EntityIdDataValue {
    #[serde(rename = "item")]
    Item(EntityIdItemInfo),

    #[serde(rename = "property")]
    Property {},

    #[serde(rename = "form")]
    Form {},

    #[serde(rename = "lexeme")]
    Lexeme {},
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TimeDataValue {}

#[derive(Serialize, Deserialize, Debug)]
pub struct MonolingualTextDataValue {}

#[derive(Serialize, Deserialize, Debug)]
pub struct GlobeCoordinateDataValue {}

#[derive(Serialize, Deserialize, Debug)]
pub struct QuantityDataValue {}

/// `DataValue` holds value and type of the data.
#[allow(clippy::module_name_repetitions)]
#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", content = "value")]
pub enum DataValue {
    /// String.
    #[serde(rename = "string")]
    String(String),

    /// Entity Id.
    #[serde(rename = "wikibase-entityid")]
    WikibaseEntityId(EntityIdDataValue),

    #[serde(rename = "time")]
    Time(TimeDataValue),

    #[serde(rename = "monolingualtext")]
    MonolingualText(MonolingualTextDataValue),

    #[serde(rename = "globecoordinate")]
    GlobeCoordinate(GlobeCoordinateDataValue),

    #[serde(rename = "quantity")]
    Quantity(QuantityDataValue),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Value {
    pub datatype: String,

    pub datavalue: DataValue,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SomeValue {
    pub property: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct NoValue {}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "snaktype")]
pub enum Snak {
    #[serde(rename = "value")]
    Value(Value),

    #[serde(rename = "somevalue")]
    SomeValue(SomeValue),

    #[serde(rename = "novalue")]
    NoValue(NoValue),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Statement {
    pub mainsnak: Snak,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
pub enum Claim {
    #[serde(rename = "statement")]
    Statement(Statement),
}

/// Represents an item ("Q") entry.
#[derive(Serialize, Deserialize, Debug)]
pub struct Item {
    /// Item ID.
    pub id: String,

    /// Short names of the item in various languages.
    pub labels: HashMap<String, Label>,

    /// Descriptions of the item in various languages.
    pub descriptions: HashMap<String, Label>,

    /// PRoperties of this item.
    pub claims: HashMap<String, Vec<Claim>>,
}

impl Item {
    /// Returns ID associated with the passed property.
    fn get_entity_ids(&self, property_id: &str) -> Option<Vec<Id>> {
        if let Some(claims) = self.claims.get(property_id) {
            let mut result = Vec::<Id>::new();
            for claim in claims {
                let Claim::Statement(statement) = claim;
                if let Snak::Value(value) = &statement.mainsnak {
                    if let DataValue::WikibaseEntityId(EntityIdDataValue::Item(entity_info)) =
                        &value.datavalue
                    {
                        result.push(Id::new(entity_info.id.clone()));
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
                let Claim::Statement(statement) = claim;
                if let Snak::Value(value) = &statement.mainsnak {
                    if let DataValue::String(website) = &value.datavalue {
                        result.push(website.clone());
                    }
                }
            }
            Some(result)
        } else {
            None
        }
    }
}

impl Item {
    /// Returns IDs of entities linked with "follows" property.
    #[must_use]
    pub fn get_follows(&self) -> Option<Vec<Id>> {
        self.get_entity_ids(properties::FOLLOWS)
    }

    /// Returns IDs of entities linked with "followed by" property.
    #[must_use]
    pub fn get_followed_by(&self) -> Option<Vec<Id>> {
        self.get_entity_ids(properties::FOLLOWED_BY)
    }

    /// Returns IDs of entities linked with "manufacturer" property.
    #[must_use]
    pub fn get_manufacturer_ids(&self) -> Option<Vec<Id>> {
        self.get_entity_ids(properties::MANUFACTURER)
    }

    /// Returns IDs of entities linked with "official website" property.
    #[must_use]
    pub fn get_official_websites(&self) -> Option<Vec<String>> {
        self.get_strings(properties::OFFICIAL_WEBSITE)
    }

    /// Checks if this items is linked to the passed entity with `instalce of` property.
    ///
    /// In simpler words: chacks if this item is an instance of the passed class.
    #[must_use]
    pub fn is_instance_of(&self, class: &str) -> bool {
        if let Some(claims) = self.claims.get(properties::INSTANCE_OF) {
            for claim in claims {
                let Claim::Statement(statement) = claim;
                if let Snak::Value(value) = &statement.mainsnak {
                    if let DataValue::WikibaseEntityId(EntityIdDataValue::Item(entity_info)) =
                        &value.datavalue
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

/// Represents a property ("P") entry.
#[derive(Serialize, Deserialize, Debug)]
pub struct Property {
    /// Property ID.
    pub id: String,
}

/// Represents one entry in Wikidata.
#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
pub enum Entity {
    /// Item ("Q") entry.
    #[serde(rename = "item")]
    Item(Item),

    /// Property ("P") entry.
    #[serde(rename = "property")]
    Property(Property),
}
