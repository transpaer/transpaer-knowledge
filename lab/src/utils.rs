//! Miscellaneous utilities.

use std::collections::HashMap;

use serde::{de::Deserializer, Deserialize};

use crate::errors;

/// Verifies that the path exists and is a file.
///
/// # Errors
///
/// Returns an error if the path does not exist or is not a file.
pub fn file_exists(path: &std::path::Path) -> Result<(), errors::ConfigCheckError> {
    if !path.exists() {
        return Err(errors::ConfigCheckError::DoesNotExist(path.to_owned()));
    }
    if !path.is_file() {
        return Err(errors::ConfigCheckError::NotAFile(path.to_owned()));
    }
    Ok(())
}

/// Verifies that the path exists and is a directory.
///
/// # Errors
///
/// Returns an error if the path does not exist or is not a directory.
pub fn dir_exists(path: &std::path::Path) -> Result<(), errors::ConfigCheckError> {
    if !path.exists() {
        return Err(errors::ConfigCheckError::DoesNotExist(path.to_owned()));
    }
    if !path.is_dir() {
        return Err(errors::ConfigCheckError::NotADir(path.to_owned()));
    }
    Ok(())
}

/// Verifies that the path itself does not exist, but it's parent exists and is a directory.
///
/// # Errors
///
/// Returns an error if the path exists or the base is not a directory.
pub fn path_creatable(path: &std::path::Path) -> Result<(), errors::ConfigCheckError> {
    if path.exists() {
        return Err(errors::ConfigCheckError::AlreadyExists(path.to_owned()));
    }

    if let Some(base) = path.parent() {
        if !base.exists() {
            return Err(errors::ConfigCheckError::DoesNotExist(base.to_owned()));
        }
        if !base.is_dir() {
            return Err(errors::ConfigCheckError::NotADir(base.to_owned()));
        }
    } else {
        return Err(errors::ConfigCheckError::NoParent(path.to_owned()));
    }

    Ok(())
}

/// Verifies that the parent of the given path itself does not exist,
/// but it's parent exists and is a directory.
///
/// # Errors
///
/// Returns an error if the path exists or the base is not a directory.
pub fn parent_creatable(path: &std::path::Path) -> Result<(), errors::ConfigCheckError> {
    if let Some(base) = path.parent() {
        path_creatable(base)
    } else {
        Err(errors::ConfigCheckError::NoParent(path.to_owned()))
    }
}

/// Trims the given name and transforms it to lower case.
#[must_use]
pub fn disambiguate_name(name: &str) -> String {
    name.trim().to_lowercase()
}

/// Merges map `m2` into map `m1` by merging common entries and copping values not present in `m1`.
/// The mergind funtionality is provided via `merge::MErge` trait.
pub fn merge_hashmaps<K, V, S>(m1: &mut HashMap<K, V, S>, m2: HashMap<K, V, S>)
where
    K: Eq + std::hash::Hash,
    V: Clone + merge::Merge,
    S: std::hash::BuildHasher,
{
    for (key, value2) in m2 {
        m1.entry(key).and_modify(|value1| value1.merge(value2.clone())).or_insert_with(|| value2);
    }
}

/// Merges map `m2` into map `m1` by merging common entries and copping values not present in `m1`.
/// The merging funtionality is provided via a closure.
pub fn merge_hashmaps_with<K, V, M, S>(m1: &mut HashMap<K, V, S>, m2: HashMap<K, V, S>, m: M)
where
    K: Eq + std::hash::Hash,
    V: Clone,
    M: Fn(&mut V, &V),
    S: std::hash::BuildHasher,
{
    for (key, value2) in m2 {
        m1.entry(key).and_modify(|value1| m(value1, &value2)).or_insert_with(|| value2);
    }
}

/// Helper for deserializing `isocountry::countryCode` from alpha3 strings.
pub fn deserialize_country_code_from_alpha3<'de, D>(
    d: D,
) -> Result<isocountry::CountryCode, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(d)?;
    isocountry::CountryCode::for_alpha3(s.as_str()).map_err(serde::de::Error::custom)
}

/// Helper for deserializing `isocountry::countryCode` from alpha3 strings.
pub fn deserialize_optional_country_code_from_alpha3<'de, D>(
    d: D,
) -> Result<Option<isocountry::CountryCode>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = Option::<String>::deserialize(d)?;
    Ok(match s {
        Some(s) => Some(
            isocountry::CountryCode::for_alpha3(s.as_str()).map_err(serde::de::Error::custom)?,
        ),
        None => None,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_merge_hashmaps() {
        #[derive(Clone, Debug, PartialEq, Eq, merge::Merge)]
        struct M(#[merge(strategy = merge::num::saturating_add)] usize);

        let mut input1: HashMap<&str, M> = [("1", M(1)), ("2", M(2))].into();
        let input2: HashMap<&str, M> = [("3", M(3)), ("2", M(2))].into();
        let output: HashMap<&str, M> = [("1", M(1)), ("2", M(4)), ("3", M(3))].into();

        merge_hashmaps(&mut input1, input2);
        assert_eq!(input1, output);
    }

    #[test]
    fn test_merge_hashmaps_with() {
        let mut input1: HashMap<&str, usize> = [("1", 1), ("2", 2)].into();
        let input2: HashMap<&str, usize> = [("3", 3), ("2", 2)].into();
        let output: HashMap<&str, usize> = [("1", 1), ("2", 4), ("3", 3)].into();

        merge_hashmaps_with(&mut input1, input2, |a, b| *a += b);
        assert_eq!(input1, output);
    }
}
