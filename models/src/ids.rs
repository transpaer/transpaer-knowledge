//! This module contains definitions of some commonly used ID data types.

use std::collections::HashSet;

use serde::{de::Deserializer, ser::Serializer, Deserialize, Serialize};
use snafu::prelude::*;

/// Maximal GTIN (highest number with 14 digits).
const MAX_GTIN: usize = 99_999_999_999_999;

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

/// Represents a numerical ID.
#[derive(Debug, Clone, Eq, Hash, PartialEq, Ord, PartialOrd)]
pub struct NumId(usize);

impl NumId {
    /// Constructs a new `NumId`.
    #[must_use]
    pub const fn new(id: usize) -> Self {
        Self(id)
    }

    #[must_use]
    pub const fn get_value(&self) -> usize {
        self.0
    }
}

impl TryFrom<&str> for NumId {
    type Error = ParseIdError;

    fn try_from(string: &str) -> Result<Self, ParseIdError> {
        match string.parse::<usize>() {
            Ok(num) => Ok(Self(num)),
            Err(err) => Err(ParseIdError::num(string.to_string(), err)),
        }
    }
}

impl Serialize for NumId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.0.to_string().as_str())
    }
}

impl<'de> Deserialize<'de> for NumId {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Self::try_from(s.as_str()).map_err(serde::de::Error::custom)
    }
}

/// Represents a Global Trade Item Number.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct Gtin(usize);

impl Gtin {
    /// Constructs as new `Gtin`.
    #[must_use]
    pub fn new(number: usize) -> Self {
        Self(number)
    }

    #[must_use]
    pub fn as_number(&self) -> usize {
        self.0
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

    /// Converts the ID into `ArangoDB` ID in `gtins` collection.
    #[must_use]
    pub fn to_db_id(&self) -> String {
        format!("gtins/{}", self.to_string())
    }
}

impl ToString for Gtin {
    fn to_string(&self) -> String {
        format!("{:0>14}", self.0)
    }
}

impl TryFrom<&str> for Gtin {
    type Error = ParseIdError;

    fn try_from(string: &str) -> Result<Self, Self::Error> {
        let string = string.replace([' ', '-', '.'], "").trim_start_matches('0').to_string();
        let len = string.len();
        if !(8..=14).contains(&len) {
            return Err(ParseIdError::length(string));
        }
        match string.parse::<usize>() {
            Ok(num) => Ok(Gtin(num)),
            Err(err) => Err(ParseIdError::num(string, err)),
        }
    }
}

impl TryFrom<&String> for Gtin {
    type Error = ParseIdError;

    fn try_from(string: &String) -> Result<Self, Self::Error> {
        Self::try_from(string.as_str())
    }
}

impl TryFrom<usize> for Gtin {
    type Error = ParseIdError;

    fn try_from(num: usize) -> Result<Self, Self::Error> {
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
        serializer.serialize_str(self.to_string().as_ref())
    }
}

impl<'de> Deserialize<'de> for Gtin {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Self::try_from(s.as_str()).map_err(serde::de::Error::custom)
    }
}

/// Represents as VAT number.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct VatId(String);

impl VatId {
    /// Constructs a new `VatId`.
    #[must_use]
    pub fn new(id: String) -> Self {
        Self(id)
    }

    /// Returns reference to the inner string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
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
        let id = id.replace([' ', '-', '.'], "");

        if id.len() < 2 {
            return Err(ParseIdError::length(id));
        }

        Ok(Self(id))
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
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub enum OrganisationId {
    /// Organisations data comes from Wikidata.
    Wiki(NumId),

    /// Organisation data comes from diffretn source providing VAT ID.
    Vat(VatId),
}

impl OrganisationId {
    /// Converts the ID into `ArangoDB` ID in `organisations` collection.
    #[must_use]
    pub fn to_db_id(&self) -> String {
        format!("organisations/{}", self.to_string())
    }
}

impl ToString for OrganisationId {
    fn to_string(&self) -> String {
        match &self {
            Self::Wiki(id) => format!("Q{}", id.0),
            Self::Vat(id) => format!("V{}", id.as_str()),
        }
    }
}

impl TryFrom<&str> for OrganisationId {
    type Error = ParseIdError;

    fn try_from(string: &str) -> Result<Self, Self::Error> {
        match string.chars().next() {
            Some('Q') => Ok(Self::Wiki(NumId::try_from(&string[1..])?)),
            Some('V') => Ok(Self::Vat(VatId::try_from(&string[1..])?)),
            Some(_) => Err(ParseIdError::prefix(string.to_string())),
            None => Err(ParseIdError::length(string.to_string())),
        }
    }
}

impl From<NumId> for OrganisationId {
    fn from(id: NumId) -> Self {
        Self::Wiki(id)
    }
}

impl From<VatId> for OrganisationId {
    fn from(id: VatId) -> Self {
        Self::Vat(id)
    }
}

impl Serialize for OrganisationId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.to_string().as_ref())
    }
}

impl<'de> Deserialize<'de> for OrganisationId {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Self::try_from(s.as_str()).map_err(serde::de::Error::custom)
    }
}

/// Represents in ID of a product.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub enum ProductId {
    /// Product data comes from Wikidata.
    Wiki(NumId),

    /// Product data comes from diffretn source providing VAT ID.
    Gtin(Gtin),
}

impl ProductId {
    /// Converts the ID into `ArangoDB` ID in `products` collection.
    #[must_use]
    pub fn to_db_id(&self) -> String {
        format!("products/{}", self.to_string())
    }
}

impl ToString for ProductId {
    fn to_string(&self) -> String {
        match &self {
            Self::Wiki(id) => format!("Q{}", id.0),
            Self::Gtin(id) => format!("G{}", id.to_string()),
        }
    }
}

impl From<NumId> for ProductId {
    fn from(id: NumId) -> Self {
        Self::Wiki(id)
    }
}

impl From<Gtin> for ProductId {
    fn from(id: Gtin) -> Self {
        Self::Gtin(id)
    }
}

impl TryFrom<&str> for ProductId {
    type Error = ParseIdError;

    fn try_from(string: &str) -> Result<Self, Self::Error> {
        match string.chars().next() {
            Some('Q') => Ok(ProductId::Wiki(NumId::try_from(&string[1..])?)),
            Some('G') => Ok(ProductId::Gtin(Gtin::try_from(&string[1..])?)),
            Some(_) => Err(ParseIdError::prefix(string.to_string())),
            None => Err(ParseIdError::length(string.to_string())),
        }
    }
}

impl Serialize for ProductId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.to_string().as_ref())
    }
}

impl<'de> Deserialize<'de> for ProductId {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Self::try_from(s.as_str()).map_err(serde::de::Error::custom)
    }
}
