// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::HashSet;

/// Extracts domain from a URL.
#[must_use]
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

/// Check if the string may be an URL, and if so, try to extract the domain.
#[must_use]
pub fn extract_domain_from_str(mut string: &str) -> Option<String> {
    let mut is_url = false;
    if string.starts_with("http://") {
        string = &string[7..];
        is_url = true;
    }
    if string.starts_with("https://") {
        string = &string[8..];
        is_url = true;
    }
    if string.starts_with("www.") {
        string = &string[4..];
        is_url = true;
    }

    if is_url {
        let domain = if let Some((host, _path)) = string.split_once('/') { host } else { string };
        Some(domain.to_lowercase())
    } else {
        None
    }
}

/// Extracts domains from multiple URLs.
pub fn extract_domains_from_urls<'a, C, U>(websites: &'a C) -> HashSet<String>
where
    &'a C: std::iter::IntoIterator<Item = U>,
    U: AsRef<str>,
{
    let mut result = HashSet::<String>::new();
    for website in websites {
        result.insert(extract_domain_from_url(website.as_ref()));
    }
    result
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn test_extract_domain_from_url() {
        assert_eq!(extract_domain_from_url("example.com"), "example.com");
        assert_eq!(extract_domain_from_url("www.example.com"), "example.com");
        assert_eq!(extract_domain_from_url("http://www.example.com"), "example.com");
        assert_eq!(extract_domain_from_url("http://www.example.com/"), "example.com");
        assert_eq!(extract_domain_from_url("https://www.example.com"), "example.com");
        assert_eq!(extract_domain_from_url("www.Example.Com/a/b/c/d?e=1"), "example.com");
        assert_eq!(extract_domain_from_url("http://www.exAmplE.com/a/"), "example.com");
        assert_eq!(extract_domain_from_url("https://www.ExamPle.com/a/"), "example.com");
    }

    #[test]
    fn test_extract_domain_from_str() {
        // It's not enough to have a dot-separated string to assume it was meant to represent a domain
        assert!(extract_domain_from_str("example.com").is_none());

        assert!(extract_domain_from_str(" http://www.example.com").is_none());
        assert!(extract_domain_from_str("xhttp://www.example.com").is_none());
        assert!(extract_domain_from_str("x http://www.example.com").is_none());

        assert_eq!(extract_domain_from_str("www.example.com").unwrap(), "example.com");
        assert_eq!(extract_domain_from_str("http://www.example.com").unwrap(), "example.com");
        assert_eq!(extract_domain_from_str("http://www.example.com/").unwrap(), "example.com");
        assert_eq!(extract_domain_from_str("https://www.example.com").unwrap(), "example.com");
        assert_eq!(extract_domain_from_str("www.Example.Com/a/b/c/d?e=1").unwrap(), "example.com");
        assert_eq!(extract_domain_from_str("http://www.exAmplE.com/a/").unwrap(), "example.com");
        assert_eq!(extract_domain_from_str("https://www.ExamPle.com/a/").unwrap(), "example.com");

        // We don't validate domains
        assert_eq!(extract_domain_from_str("http://notadomain").unwrap(), "notadomain");
    }

    #[test]
    fn test_extract_domains_from_urls_vec() {
        let input = vec!["www.example.com", "http://www.example.com", "example2.com"];
        let output: HashSet<String> = ["example.com".into(), "example2.com".into()].into();
        assert_eq!(extract_domains_from_urls(&input), output);
    }

    #[test]
    fn test_extract_domains_from_urls_hashset() {
        let input: HashSet<String> =
            ["www.example.com".into(), "http://example.com".into(), "example2.com".into()].into();
        let output: HashSet<String> = ["example.com".into(), "example2.com".into()].into();
        assert_eq!(extract_domains_from_urls(&input), output);
    }
}
