//! This module contains definitions of some commonly used ID data types.

use std::collections::HashSet;

use serde::{de::Deserializer, ser::Serializer, Deserialize, Serialize};
use snafu::prelude::*;

/// Maximal EAN (highest number with 13 digits).
const MAX_EAN: u64 = 9_999_999_999_999;

/// Maximal GTIN (highest number with 14 digits).
const MAX_GTIN: u64 = 99_999_999_999_999;

/// Describes an error occured during parsing an Id.
#[derive(Debug, Eq, PartialEq, Snafu)]
pub enum ParseIdError {
    /// Part of the ID was expected to be a number but wasn't.
    #[snafu(display("Failed to parse number from `{string}`: {source}"))]
    Num { source: std::num::ParseIntError, string: String },

    /// Length of the ID was wrong.
    #[snafu(display("The ID `{string}` has wrong length"))]
    Length { string: String },

    /// The ID didn't contain the expected prefix.
    #[snafu(display("The ID `{string}` has unexpected prefix"))]
    Prefix { string: String },
}

impl ParseIdError {
    pub fn num(string: String, source: std::num::ParseIntError) -> Self {
        Self::Num { string, source }
    }

    pub fn length(string: String) -> Self {
        Self::Length { string }
    }

    pub fn prefix(string: String) -> Self {
        Self::Prefix { string }
    }
}

impl From<transpaer_wikidata::errors::ParseIdError> for ParseIdError {
    fn from(err: transpaer_wikidata::errors::ParseIdError) -> Self {
        use transpaer_wikidata::errors::ParseIdError as E;
        match err {
            E::Num(string, err) => Self::Num { string, source: err },
            E::Length(string) => Self::Length { string },
            E::Prefix(string) => Self::Prefix { string },
        }
    }
}

/// Represents a Wikidata ID in a numeric form.
///
/// Compare to `StrId`. Numenric ID takes less memory and is easier to compare, but string form is
/// sometimes easier to handle.
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct WikiId(u64);

impl WikiId {
    /// Constructs a new `Id`.
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    #[must_use]
    pub fn as_value(&self) -> u64 {
        self.0
    }

    #[must_use]
    pub fn to_canonical_string(&self) -> String {
        self.as_value().to_string()
    }
}

impl From<transpaer_wikidata::data::Id> for WikiId {
    fn from(other: transpaer_wikidata::data::Id) -> Self {
        Self(other.get_value())
    }
}

impl TryFrom<&str> for WikiId {
    type Error = ParseIdError;

    fn try_from(string: &str) -> Result<Self, ParseIdError> {
        match string.parse::<u64>() {
            Ok(num) => Ok(Self(num)),
            Err(err) => Err(ParseIdError::num(string.to_string(), err)),
        }
    }
}

impl TryFrom<&String> for WikiId {
    type Error = ParseIdError;

    fn try_from(string: &String) -> Result<Self, Self::Error> {
        Self::try_from(string.as_str())
    }
}

impl Serialize for WikiId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for WikiId {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let value = u64::deserialize(d)?;
        Ok(Self::new(value))
    }
}

/// Represents a Internationl Article Number.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct Ean(u64);

impl Ean {
    /// Constructs as new `Ean`.
    #[must_use]
    pub fn new(number: u64) -> Self {
        Self(number)
    }

    #[must_use]
    pub fn as_value(&self) -> u64 {
        self.0
    }

    #[must_use]
    pub fn to_canonical_string(&self) -> String {
        self.0.to_string()
    }
}

impl std::fmt::Display for Ean {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<&str> for Ean {
    type Error = ParseIdError;

    fn try_from(string: &str) -> Result<Self, Self::Error> {
        let string = string.replace([' ', '-', '.'], "").trim_start_matches('0').to_string();
        match string.parse::<u64>() {
            Ok(num) => Ok(Ean::try_from(num)?),
            Err(err) => Err(ParseIdError::num(string, err)),
        }
    }
}

impl TryFrom<&String> for Ean {
    type Error = ParseIdError;

    fn try_from(string: &String) -> Result<Self, Self::Error> {
        Self::try_from(string.as_str())
    }
}

impl TryFrom<u64> for Ean {
    type Error = ParseIdError;

    fn try_from(num: u64) -> Result<Self, Self::Error> {
        if num > MAX_EAN {
            return Err(ParseIdError::length(num.to_string()));
        }
        Ok(Self(num))
    }
}

impl Serialize for Ean {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.as_value())
    }
}

impl<'de> Deserialize<'de> for Ean {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let value = u64::deserialize(d)?;
        Ok(Self::new(value))
    }
}

/// Represents a Global Trade Item Number.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct Gtin(u64);

impl Gtin {
    /// Constructs as new `Gtin`.
    #[must_use]
    pub const fn new(number: u64) -> Self {
        Self(number)
    }

    #[must_use]
    pub fn as_value(&self) -> u64 {
        self.0
    }

    #[must_use]
    pub fn to_canonical_string(&self) -> String {
        format!("{:0>14}", self.0)
    }

    /// Converts optional vector of strings to a vector of VAT IDs.
    ///
    /// # Errors
    ///
    /// Returns an error if at least one of the strings could not be parsed as a VAT ID.
    pub fn convert(data: Option<Vec<String>>) -> Result<HashSet<Self>, ParseIdError> {
        match data {
            Some(ids) => {
                let mut result = HashSet::with_capacity(ids.len());
                for id in ids {
                    result.insert(Self::try_from(id.as_str())?);
                }
                Ok(result)
            }
            None => Ok(HashSet::default()),
        }
    }
}

impl std::fmt::Display for Gtin {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:0>14}", self.0)
    }
}

impl TryFrom<&str> for Gtin {
    type Error = ParseIdError;

    fn try_from(gtin: &str) -> Result<Self, Self::Error> {
        let mut reduced = gtin.to_owned();
        reduced.retain(|c| c.is_ascii_alphanumeric());
        let len = reduced.len();
        if !(8..=14).contains(&len) {
            return Err(ParseIdError::length(gtin.to_owned()));
        }
        match reduced.parse::<u64>() {
            Ok(num) => Ok(Gtin(num)),
            Err(err) => Err(ParseIdError::num(reduced, err)),
        }
    }
}

impl TryFrom<&String> for Gtin {
    type Error = ParseIdError;

    fn try_from(string: &String) -> Result<Self, Self::Error> {
        Self::try_from(string.as_str())
    }
}

impl TryFrom<u64> for Gtin {
    type Error = ParseIdError;

    fn try_from(num: u64) -> Result<Self, Self::Error> {
        if num > MAX_GTIN {
            return Err(ParseIdError::length(num.to_string()));
        }
        Ok(Self(num))
    }
}

impl Serialize for Gtin {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.as_value())
    }
}

impl<'de> Deserialize<'de> for Gtin {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let value = u64::deserialize(d)?;
        Ok(Self::new(value))
    }
}

/// Represents ASIN number.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct Asin(String);

impl Asin {
    /// Constructs a new `VatId`.
    #[must_use]
    pub fn new(id: &str) -> Self {
        Self(id.to_owned())
    }

    /// Returns reference to the inner string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// Returns reference to the inner string.
    #[must_use]
    pub fn to_canonical_string(&self) -> String {
        self.0.clone()
    }
}

impl From<&str> for Asin {
    fn from(id: &str) -> Self {
        Self::new(id)
    }
}

impl From<&String> for Asin {
    fn from(id: &String) -> Self {
        Self::from(id.as_str())
    }
}

impl Serialize for Asin {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Asin {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Ok(Self::from(s.as_str()))
    }
}

/// Represents a VAT number.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct VatId(String);

impl VatId {
    /// Constructs a new `VatId`.
    #[must_use]
    pub fn new(id: &str) -> Self {
        let mut reduced = id.to_owned();
        reduced.retain(|c| c.is_ascii_alphanumeric());
        Self(reduced)
    }

    /// Returns reference to the inner string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// Returns reference to the inner string.
    #[must_use]
    pub fn to_canonical_string(&self) -> String {
        self.0.clone()
    }

    /// Converts optional vector of strings to a vector of VAT IDs.
    ///
    /// # Errors
    ///
    /// Returns an error if at least one of the strings could not be parsed as a VAT ID.
    pub fn convert(data: Option<Vec<String>>) -> Result<HashSet<Self>, ParseIdError> {
        match data {
            Some(ids) => {
                let mut result = HashSet::with_capacity(ids.len());
                for id in ids {
                    result.insert(Self::try_from(id.as_str())?);
                }
                Ok(result)
            }
            None => Ok(HashSet::default()),
        }
    }
}

impl TryFrom<&str> for VatId {
    type Error = ParseIdError;

    fn try_from(id: &str) -> Result<Self, Self::Error> {
        let vat = Self::new(id);

        if vat.0.len() < 2 {
            return Err(ParseIdError::length(vat.0));
        }

        Ok(vat)
    }
}

impl TryFrom<&String> for VatId {
    type Error = ParseIdError;

    fn try_from(id: &String) -> Result<Self, Self::Error> {
        Self::try_from(id.as_str())
    }
}

impl Serialize for VatId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for VatId {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Self::try_from(s.as_str()).map_err(serde::de::Error::custom)
    }
}

/// Represents in ID of an organisation.
#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct OrganisationId(u32);

impl OrganisationId {
    /// Constructs a new `OrganisationId`.
    pub fn from_value(value: u32) -> Self {
        Self(value)
    }

    /// Returns the underlying value.
    pub fn as_value(&self) -> u32 {
        self.0
    }

    // Converts the ID to string for serialisation.
    pub fn to_canonical_string(&self) -> String {
        self.0.to_string()
    }
}

impl std::fmt::Display for OrganisationId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Represents in ID of a product.
#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct ProductId(u32);

impl ProductId {
    /// Constructs a new `ProductId`.
    pub fn from_value(value: u32) -> Self {
        Self(value)
    }

    /// Returns the underlying value.
    pub fn as_value(&self) -> u32 {
        self.0
    }

    // Converts the ID to string for serialisation.
    pub fn to_canonical_string(&self) -> String {
        self.0.to_string()
    }
}

impl std::fmt::Display for ProductId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
