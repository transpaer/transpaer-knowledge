//! This module contains definitions of some commonly used data types.

use serde::{de::Deserializer, ser::Serializer, Deserialize, Serialize};

pub use sustainity_wikidata::{
    data::{Id as WikiId, StrId as WikiStrId},
    errors::ParseIdError,
};

/// Maximal GTIN (highest number with 14 digits).
const MAX_GTIN: usize = 99_999_999_999_999;

/// Represents a Global Trade Item Number.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct Gtin(usize);

impl Gtin {
    /// Constructs as new `Gtin`.
    #[must_use]
    pub fn new(number: usize) -> Self {
        Self(number)
    }

    /// Converts optional vector of strings to a vector of VAT IDs.
    ///
    /// # Errors
    ///
    /// Returns an error if at least one of the strings could not be parsed as a VAT ID.
    pub fn convert(data: Option<Vec<String>>) -> Result<Vec<Self>, ParseIdError> {
        match data {
            Some(ids) => {
                let mut result = Vec::with_capacity(ids.len());
                for id in ids {
                    result.push(Self::try_from(id.as_str())?);
                }
                Ok(result)
            }
            None => Ok(Vec::default()),
        }
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
        let string = string.replace([' ', '-', '.'], "");
        let len = string.len();
        if len != 8 && len != 12 && len != 13 && len != 14 {
            return Err(ParseIdError::Length(string));
        }
        match string.parse::<usize>() {
            Ok(num) => Ok(Gtin(num)),
            Err(err) => Err(ParseIdError::Num(string.to_string(), err)),
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
            return Err(ParseIdError::Length(num.to_string()));
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
    pub fn convert(data: Option<Vec<String>>) -> Result<Vec<Self>, ParseIdError> {
        match data {
            Some(ids) => {
                let mut result = Vec::with_capacity(ids.len());
                for id in ids {
                    result.push(Self::try_from(id.as_str())?);
                }
                Ok(result)
            }
            None => Ok(Vec::default()),
        }
    }
}

impl TryFrom<&str> for VatId {
    type Error = ParseIdError;

    fn try_from(id: &str) -> Result<Self, Self::Error> {
        let id = id.replace([' ', '-', '.'], "");

        if id.len() < 4 {
            return Err(ParseIdError::Length(id));
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
    Wiki(WikiId),

    /// Organisation data comes from diffretn source providing VAT ID.
    Vat(VatId),
}

impl OrganisationId {
    /// Converts optional vector of string Wikidata IDs to a vector of Organisation IDs.
    ///
    /// # Errors
    ///
    /// Returns an error if at least one of the strings could not be parsed as an ID.
    pub fn convert(
        data: Option<Vec<WikiStrId>>,
    ) -> Result<Option<Vec<OrganisationId>>, ParseIdError> {
        if let Some(ids) = data {
            let mut result = Vec::with_capacity(ids.len());
            for id in ids {
                result.push(Self::Wiki(id.to_num_id()?));
            }
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }
}

impl ToString for OrganisationId {
    fn to_string(&self) -> String {
        match &self {
            Self::Wiki(id) => id.to_str_id().into_string(),
            Self::Vat(id) => format!("V{}", id.as_str()),
        }
    }
}

impl From<WikiId> for OrganisationId {
    fn from(id: WikiId) -> Self {
        Self::Wiki(id)
    }
}

impl TryFrom<WikiStrId> for OrganisationId {
    type Error = ParseIdError;

    fn try_from(id: WikiStrId) -> Result<Self, Self::Error> {
        Ok(Self::Wiki(id.to_num_id()?))
    }
}

impl TryFrom<&str> for OrganisationId {
    type Error = ParseIdError;

    fn try_from(string: &str) -> Result<Self, Self::Error> {
        match string.chars().next() {
            Some('Q') => Ok(Self::Wiki(WikiId::try_from(string)?)),
            Some('V') => Ok(Self::Vat(VatId::try_from(&string[1..])?)),
            Some(_) => Err(ParseIdError::Prefix(string.to_string())),
            None => Err(ParseIdError::Length(string.to_string())),
        }
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
    Wiki(WikiId),

    /// Product data comes from diffretn source providing VAT ID.
    Gtin(Gtin),
}

impl ProductId {
    /// Converts optional vector of string IDs to a vector of product IDs.
    ///
    /// # Errors
    ///
    /// Returns an error if at least one of the strings could not be parsed as an ID.
    pub fn convert(data: Option<Vec<WikiStrId>>) -> Result<Option<Vec<ProductId>>, ParseIdError> {
        if let Some(ids) = data {
            let mut result = Vec::with_capacity(ids.len());
            for id in ids {
                result.push(Self::Wiki(id.to_num_id()?));
            }
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }
}

impl ToString for ProductId {
    fn to_string(&self) -> String {
        match &self {
            Self::Wiki(id) => id.to_str_id().into_string(),
            Self::Gtin(id) => format!("G{}", id.to_string()),
        }
    }
}

impl From<WikiId> for ProductId {
    fn from(id: WikiId) -> Self {
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
            Some('Q') => Ok(ProductId::Wiki(WikiId::try_from(string)?)),
            Some('G') => Ok(ProductId::Gtin(Gtin::try_from(&string[1..])?)),
            Some(_) => Err(ParseIdError::Prefix(string.to_string())),
            None => Err(ParseIdError::Length(string.to_string())),
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
