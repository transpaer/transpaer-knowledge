//! Structures and enums found in this module represents data found in the Wikidata dump file.
//!
//! Many of the stuctures and enums are not finished!

use std::collections::HashMap;

use serde::{de::Deserializer, ser::Serializer, Deserialize, Serialize};

use crate::errors::ParseIdError;

/// Represents a Wikidata ID in a string form.
///
/// Compare to `Id`. Numenric ID takes less memory and is easier to compare, but string form is
/// sometimes easier to handle.
#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct StrId(String);

impl StrId {
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

    /// Parses the string ID.
    ///
    /// # Errors
    ///
    /// Returns error if the parsing failed.
    pub fn to_num_id(&self) -> Result<Id, ParseIdError> {
        Id::try_from(&self.0)
    }
}

impl From<String> for StrId {
    fn from(string: String) -> Self {
        Self(string)
    }
}

impl PartialEq<&str> for StrId {
    fn eq(&self, other: &&str) -> bool {
        self.as_str().eq(*other)
    }
}

impl PartialEq<StrId> for &str {
    fn eq(&self, other: &StrId) -> bool {
        (*self).eq(other.as_str())
    }
}

/// Represents a Wikidata ID in a numeric form.
///
/// Compare to `StrId`. Numenric ID takes less memory and is easier to compare, but string form is
/// sometimes easier to handle.
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct Id(u64);

impl Id {
    /// Constructs a new `Id`.
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Returns the `Id` in the "string" form.
    #[must_use]
    pub fn to_str_id(&self) -> StrId {
        StrId(format!("Q{}", self.0))
    }

    #[must_use]
    pub fn get_value(&self) -> u64 {
        self.0
    }

    #[must_use]
    pub fn to_id(&self) -> String {
        self.get_value().to_string()
    }
}

impl TryFrom<&str> for Id {
    type Error = ParseIdError;

    fn try_from(string: &str) -> Result<Self, ParseIdError> {
        match string.chars().next() {
            Some(char) => {
                if char != 'Q' {
                    return Err(ParseIdError::Prefix(string.to_string()));
                }
            }
            None => {
                return Err(ParseIdError::Length(string.to_string()));
            }
        }

        match string[1..].parse::<u64>() {
            Ok(num) => Ok(Self(num)),
            Err(err) => Err(ParseIdError::Num(string.to_string(), err)),
        }
    }
}

impl TryFrom<&String> for Id {
    type Error = ParseIdError;

    fn try_from(string: &String) -> Result<Self, Self::Error> {
        Self::try_from(string.as_str())
    }
}

impl Serialize for Id {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.get_value())
    }
}

impl Id {
    /// Deserializes the `Id` from a string.
    ///
    /// # Errors
    ///
    /// Returns an error if the ID is not valid.
    pub fn deserialize_from_string<'de, D>(d: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(d)?;
        Self::try_from(s.as_str()).map_err(serde::de::Error::custom)
    }

    /// Deserializes the `Id` from an integer.
    ///
    /// # Errors
    ///
    /// Returns an error if the ID is not valid.
    pub fn deserialize_from_integer<'de, D>(d: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u64::deserialize(d)?;
        Ok(Self::new(value))
    }
}

/// Deserializes a vector of `Id`s from an array of string.
///
/// # Errors
///
/// Returns an error if any of the IDs in the array is invalid.
pub fn deserialize_vec_id_from_vec_string<'de, D>(d: D) -> Result<Vec<Id>, D::Error>
where
    D: Deserializer<'de>,
{
    let vec_string = Vec::<String>::deserialize(d)?;
    let mut vec_id = Vec::new();
    for string in vec_string {
        vec_id.push(Id::try_from(&string).map_err(serde::de::Error::custom)?);
    }
    Ok(vec_id)
}

/// Deserializes a vector of `Id`s from an array of integers.
///
/// # Errors
///
/// Returns an error if any of the IDs in the array is invalid.
pub fn deserialize_vec_id_from_vec_integer<'de, D>(d: D) -> Result<Vec<Id>, D::Error>
where
    D: Deserializer<'de>,
{
    let vec_integer = Vec::<u64>::deserialize(d)?;
    let mut vec_id = Vec::new();
    for integer in vec_integer {
        vec_id.push(Id::new(integer));
    }
    Ok(vec_id)
}

/// Deserializes an optional `Id` from an optional string.
///
/// # Errors
///
/// Returns an error if the ID is not valid.
pub fn deserialize_option_id_from_option_string<'de, D>(d: D) -> Result<Option<Id>, D::Error>
where
    D: Deserializer<'de>,
{
    if let Some(string) = Option::<String>::deserialize(d)? {
        Ok(Some(Id::try_from(&string).map_err(serde::de::Error::custom)?))
    } else {
        Ok(None)
    }
}

/// All supported languages.
pub enum Language {
    En,
}

impl Language {
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::En => "en",
        }
    }
}

/// Represents Wikidata redirection.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Redirection {
    from: StrId,
    to: StrId,
}

/// Represents a Wikidata label.
#[derive(Serialize, Deserialize, Debug, Clone)]
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
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct EntityIdInfo {
    /// Full ID.
    pub id: StrId,

    /// Number from the ID without the prefix.
    #[serde(rename = "numeric-id")]
    pub numeric_id: u64,
}

/// Represents a Wikidata entity ID.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct EntityIdOptionInfo {
    /// Full ID.
    pub id: Option<String>,

    /// Number from the ID without the prefix.
    #[serde(rename = "numeric-id")]
    pub numeric_id: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
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

#[derive(Serialize, Deserialize, Debug, Clone)]
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

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct MonolingualTextDataValue {
    #[serde(rename = "text")]
    pub text: String,

    #[serde(rename = "language")]
    pub language: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
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

#[derive(Serialize, Deserialize, Debug, Clone)]
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
#[derive(Serialize, Deserialize, Debug, Clone)]
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

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Value {
    #[serde(rename = "hash")]
    pub hash: Option<String>,

    #[serde(rename = "property")]
    pub property: String,

    #[serde(rename = "datatype")]
    pub datatype: Option<String>,

    #[serde(rename = "datavalue")]
    pub datavalue: DataValue,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct SomeValue {
    #[serde(rename = "hash")]
    pub hash: Option<String>,

    #[serde(rename = "property")]
    pub property: String,

    #[serde(rename = "datatype")]
    pub datatype: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct NoValue {
    #[serde(rename = "hash")]
    pub hash: Option<String>,

    #[serde(rename = "property")]
    pub property: String,

    #[serde(rename = "datatype")]
    pub datatype: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "snaktype", deny_unknown_fields)]
pub enum Snak {
    #[serde(rename = "value")]
    Value(Value),

    #[serde(rename = "somevalue")]
    SomeValue(SomeValue),

    #[serde(rename = "novalue")]
    NoValue(NoValue),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub enum Rank {
    #[serde(rename = "preferred")]
    Preferred,

    #[serde(rename = "normal")]
    Normal,

    #[serde(rename = "deprecated")]
    Deprecated,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Reference {
    #[serde(rename = "hash")]
    hash: String,

    #[serde(rename = "snaks")]
    snaks: HashMap<StrId, Vec<Snak>>,

    #[serde(rename = "snaks-order")]
    snaks_order: Vec<StrId>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Statement {
    #[serde(rename = "id")]
    pub id: String,

    #[serde(rename = "mainsnak")]
    pub mainsnak: Snak,

    #[serde(rename = "rank")]
    pub rank: Rank,

    #[serde(rename = "qualifiers")]
    pub qualifiers: Option<HashMap<StrId, Vec<Snak>>>,

    #[serde(rename = "qualifiers-order")]
    pub qualifiers_order: Option<Vec<StrId>>,

    #[serde(rename = "references")]
    pub references: Option<Vec<Reference>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum Claim {
    #[serde(rename = "statement")]
    Statement(Statement),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Sitelink {
    pub site: String,
    pub title: String,
    pub badges: Vec<StrId>,
}

/// Represents an item ("Q") entry.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Item {
    /// Item ID.
    #[serde(deserialize_with = "Id::deserialize_from_string")]
    pub id: Id,

    pub title: Option<String>,
    pub pageid: Option<u64>,
    pub ns: Option<u64>,
    pub lastrevid: u64,
    pub modified: Option<String>,

    /// Redirection.
    pub redirects: Option<Redirection>,

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
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Property {
    /// Property ID.
    pub id: String,

    pub title: Option<String>,
    pub pageid: Option<u64>,
    pub ns: Option<u64>,
    pub lastrevid: u64,
    pub modified: Option<String>,
    pub datatype: Option<String>,

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
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum Entity {
    /// Item ("Q") entry.
    #[serde(rename = "item")]
    Item(Item),

    /// Property ("P") entry.
    #[serde(rename = "property")]
    Property(Property),
}
