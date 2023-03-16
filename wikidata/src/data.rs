//! Structures and enums found in this module represents data found in the Wikidata dump file.
//!
//! Many of the stuctures and enums are not finished!

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct EntityIdInfo {
    /// Full ID.
    pub id: String,

    /// Number from the ID without the prefix.
    #[serde(rename = "numeric-id")]
    pub numeric_id: u64,
}

/// Represents a Wikidata entity ID.
#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct EntityIdOptionInfo {
    /// Full ID.
    pub id: Option<String>,

    /// Number from the ID without the prefix.
    #[serde(rename = "numeric-id")]
    pub numeric_id: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "entity-type", deny_unknown_fields)]
pub enum EntityIdDataValue {
    #[serde(rename = "item")]
    Item(EntityIdInfo),

    #[serde(rename = "property")]
    Property(EntityIdInfo),

    #[serde(rename = "form")]
    Form(EntityIdOptionInfo),

    #[serde(rename = "lexeme")]
    Lexeme(EntityIdOptionInfo),

    #[serde(rename = "sense")]
    Sense(EntityIdOptionInfo),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct TimeDataValue {
    #[serde(rename = "time")]
    pub time: String,

    #[serde(rename = "timezone")]
    pub timezone: i64,

    #[serde(rename = "before")]
    pub before: i64,

    #[serde(rename = "after")]
    pub after: i64,

    #[serde(rename = "precision")]
    pub precision: i64,

    #[serde(rename = "calendarmodel")]
    pub calendarmodel: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct MonolingualTextDataValue {
    #[serde(rename = "text")]
    pub text: String,

    #[serde(rename = "language")]
    pub language: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct GlobeCoordinateDataValue {
    #[serde(rename = "latitude")]
    pub latitude: f64,

    #[serde(rename = "longitude")]
    pub longitude: f64,

    #[serde(rename = "altitude")]
    pub altitude: Option<f64>,

    #[serde(rename = "precision")]
    pub precision: Option<f64>,

    #[serde(rename = "globe")]
    pub globe: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct QuantityDataValue {
    #[serde(rename = "amount")]
    pub amount: String,

    #[serde(rename = "upperBound")]
    pub upper_bound: Option<String>,

    #[serde(rename = "lowerBound")]
    pub lower_bound: Option<String>,

    #[serde(rename = "unit")]
    pub unit: String,
}

/// `DataValue` holds value and type of the data.
#[allow(clippy::module_name_repetitions)]
#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", content = "value", deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct Value {
    #[serde(rename = "hash")]
    pub hash: Option<String>,

    #[serde(rename = "property")]
    pub property: String,

    #[serde(rename = "datatype")]
    pub datatype: String,

    #[serde(rename = "datavalue")]
    pub datavalue: DataValue,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct SomeValue {
    #[serde(rename = "hash")]
    pub hash: Option<String>,

    #[serde(rename = "property")]
    pub property: String,

    #[serde(rename = "datatype")]
    pub datatype: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct NoValue {
    #[serde(rename = "hash")]
    pub hash: Option<String>,

    #[serde(rename = "property")]
    pub property: String,

    #[serde(rename = "datatype")]
    pub datatype: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "snaktype", deny_unknown_fields)]
pub enum Snak {
    #[serde(rename = "value")]
    Value(Value),

    #[serde(rename = "somevalue")]
    SomeValue(SomeValue),

    #[serde(rename = "novalue")]
    NoValue(NoValue),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub enum Rank {
    #[serde(rename = "preferred")]
    Preferred,

    #[serde(rename = "normal")]
    Normal,

    #[serde(rename = "deprecated")]
    Deprecated,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Reference {
    #[serde(rename = "hash")]
    hash: String,

    #[serde(rename = "snaks")]
    snaks: HashMap<Id, Vec<Snak>>,

    #[serde(rename = "snaks-order")]
    snaks_order: Vec<Id>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Statement {
    #[serde(rename = "id")]
    pub id: String,

    #[serde(rename = "mainsnak")]
    pub mainsnak: Snak,

    #[serde(rename = "rank")]
    pub rank: Rank,

    #[serde(rename = "qualifiers")]
    pub qualifiers: Option<HashMap<Id, Vec<Snak>>>,

    #[serde(rename = "qualifiers-order")]
    pub qualifiers_order: Option<Vec<Id>>,

    #[serde(rename = "references")]
    pub references: Option<Vec<Reference>>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum Claim {
    #[serde(rename = "statement")]
    Statement(Statement),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Sitelink {
    pub site: String,
    pub title: String,
    pub badges: Vec<Id>,
}

/// Represents an item ("Q") entry.
#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Item {
    /// Item ID.
    pub id: String,

    pub title: String,
    pub pageid: u64,
    pub ns: u64,
    pub lastrevid: u64,
    pub modified: String,

    /// Short names of the item in various languages.
    pub labels: HashMap<String, Label>,

    /// Descriptions of the item in various languages.
    pub descriptions: HashMap<String, Label>,

    /// Aliases of the item in various languages.
    pub aliases: HashMap<String, Vec<Label>>,

    /// Properties of this item.
    pub claims: HashMap<String, Vec<Claim>>,

    /// Sitelinks.
    pub sitelinks: HashMap<String, Sitelink>,
}

/// Represents a property ("P") entry.
#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Property {
    /// Property ID.
    pub id: String,

    pub title: String,
    pub pageid: u64,
    pub ns: u64,
    pub lastrevid: u64,
    pub modified: String,
    pub datatype: String,

    /// Short names of the item in various languages.
    pub labels: HashMap<String, Label>,

    /// Descriptions of the item in various languages.
    pub descriptions: HashMap<String, Label>,

    /// Aliases of the item in various languages.
    pub aliases: HashMap<String, Vec<Label>>,

    /// Properties of this item.
    pub claims: HashMap<String, Vec<Claim>>,
}

/// Represents one entry in Wikidata.
#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum Entity {
    /// Item ("Q") entry.
    #[serde(rename = "item")]
    Item(Item),

    /// Property ("P") entry.
    #[serde(rename = "property")]
    Property(Property),
}
