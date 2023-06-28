//! Miscellaneous utilities.

use std::collections::HashMap;

use crate::errors;

/// Extracts domain from a URL.
pub fn extract_domain_from_url(url: &str) -> String {
    let mut domain = url;
    if domain.starts_with("http://") {
        domain = &domain[7..];
    }
    if domain.starts_with("https://") {
        domain = &domain[8..];
    }
    if domain.starts_with("www.") {
        domain = &domain[4..];
    }
    if let Some((host, _path)) = domain.split_once('/') {
        domain = host;
    }
    domain.to_lowercase()
}

/// Extracts domains from multiple URLs.
pub fn extract_domains_from_urls<'a, C, U>(websites: &'a C) -> std::collections::HashSet<String>
where
    &'a C: std::iter::IntoIterator<Item = U>,
    U: AsRef<str>,
{
    let mut result = std::collections::HashSet::<String>::new();
    for website in websites {
        result.insert(extract_domain_from_url(website.as_ref()));
    }
    result
}

/// Checks is the path exists and is a file.
pub fn is_path_ok(path: &std::path::Path) -> bool {
    path.exists() && path.is_file()
}

/// Verifies that the path exists and is a file.
pub fn path_exists(path: &std::path::Path) -> Result<(), errors::ConfigCheckError> {
    if !path.exists() {
        return Err(errors::ConfigCheckError::PathDoesNotExist(path.to_owned()));
    }
    if !path.is_file() {
        return Err(errors::ConfigCheckError::PathIsNotAFile(path.to_owned()));
    }
    Ok(())
}

/// Verifies that the path exists and is a directory.
pub fn dir_exists(path: &std::path::Path) -> Result<(), errors::ConfigCheckError> {
    if !path.exists() {
        return Err(errors::ConfigCheckError::PathDoesNotExist(path.to_owned()));
    }
    if !path.is_dir() {
        return Err(errors::ConfigCheckError::PathIsNotADir(path.to_owned()));
    }
    Ok(())
}

/// Verifies that the path itself does not exist, but it's parent exists and is a directory.
pub fn path_creatable(path: &std::path::Path) -> Result<(), errors::ConfigCheckError> {
    if path.exists() {
        return Err(errors::ConfigCheckError::PathAlreadyExists(path.to_owned()));
    }

    if let Some(base) = path.parent() {
        if !base.exists() {
            return Err(errors::ConfigCheckError::BaseDoesNotExist(path.to_owned()));
        }
        if !base.is_dir() {
            return Err(errors::ConfigCheckError::BaseIsNotADirectory(path.to_owned()));
        }
    } else {
        return Err(errors::ConfigCheckError::BaseDoesNotExist(path.to_owned()));
    }

    Ok(())
}

/// Formats duration to a human-readable format.
pub fn format_elapsed_time(duration: std::time::Duration) -> String {
    let duration = duration.as_secs();
    let seconds = duration % 60;
    let minutes = (duration / 60) % 60;
    let hours = duration / 3600;
    format!("{hours}h {minutes}m {seconds}s")
}

/// Merges map `m2` into map `m1` by merging common entries and copping values not present in `m1`.
/// The mergind funtionality is provided via `merge::MErge` trait.
pub fn merge_hashmaps<K, V>(m1: &mut HashMap<K, V>, m2: HashMap<K, V>)
where
    K: Eq + std::hash::Hash,
    V: Clone + merge::Merge,
{
    for (key, value2) in m2 {
        m1.entry(key).and_modify(|value1| value1.merge(value2.clone())).or_insert_with(|| value2);
    }
}

/// Merges map `m2` into map `m1` by merging common entries and copping values not present in `m1`.
/// The mergind funtionality is provided via a closure.
pub fn merge_hashmaps_with<K, V, M>(m1: &mut HashMap<K, V>, m2: HashMap<K, V>, m: M)
where
    K: Eq + std::hash::Hash,
    V: Clone,
    M: Fn(&mut V, V),
{
    for (key, value2) in m2 {
        m1.entry(key).and_modify(|value1| m(value1, value2.clone())).or_insert_with(|| value2);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::*;

    #[test]
    fn test_extract_domain_from_url() {
        assert_eq!(extract_domain_from_url("example.com"), "example.com");
        assert_eq!(extract_domain_from_url("www.example.com"), "example.com");
        assert_eq!(extract_domain_from_url("http://www.example.com"), "example.com");
        assert_eq!(extract_domain_from_url("https://www.example.com"), "example.com");
        assert_eq!(extract_domain_from_url("www.Example.Com/a/b/c/d?e=1"), "example.com");
        assert_eq!(extract_domain_from_url("http://www.exAmplE.com/a/"), "example.com");
        assert_eq!(extract_domain_from_url("https://www.ExamPle.com/a/"), "example.com");
    }

    #[test]
    fn test_extract_domains_from_urls_vec() {
        let input = vec!["www.example.com", "http://www.example.com", "example2.com"];
        let output: HashSet<String> = ["example.com".into(), "example2.com".into()].into();
        assert_eq!(extract_domains_from_urls(&input), output);
    }

    #[test]
    fn test_extract_domains_from_urls_hashmap() {
        let input: HashSet<String> =
            ["www.example.com".into(), "http://example.com".into(), "example2.com".into()].into();
        let output: HashSet<String> = ["example.com".into(), "example2.com".into()].into();
        assert_eq!(extract_domains_from_urls(&input), output);
    }

    #[test]
    fn test_format_elapsed_time() {
        use std::time::Duration;

        assert_eq!(format_elapsed_time(Duration::new(0, 0)), "0h 0m 0s");
        assert_eq!(format_elapsed_time(Duration::new(12, 0)), "0h 0m 12s");
        assert_eq!(format_elapsed_time(Duration::new(120, 0)), "0h 2m 0s");
        assert_eq!(format_elapsed_time(Duration::new(134, 0)), "0h 2m 14s");
        assert_eq!(format_elapsed_time(Duration::new(3600, 0)), "1h 0m 0s");
        assert_eq!(format_elapsed_time(Duration::new(3720, 0)), "1h 2m 0s");
        assert_eq!(format_elapsed_time(Duration::new(3724, 0)), "1h 2m 4s");
    }

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
