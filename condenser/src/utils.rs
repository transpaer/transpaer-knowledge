//! Miscellaneous utilities.

/// Extracs domain from a URL.
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
    if let Some((host, _path)) = domain.split_once("/") {
        domain = host;
    }
    domain.to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add() {
        assert_eq!(extract_domain_from_url("example.com"), "example.com");
        assert_eq!(extract_domain_from_url("www.example.com"), "example.com");
        assert_eq!(extract_domain_from_url("http://www.example.com"), "example.com");
        assert_eq!(extract_domain_from_url("https://www.example.com"), "example.com");
        assert_eq!(extract_domain_from_url("www.Example.Com/a/b/c/d?e=1"), "example.com");
        assert_eq!(extract_domain_from_url("http://www.exAmplE.com/a/"), "example.com");
        assert_eq!(extract_domain_from_url("https://www.ExamPle.com/a/"), "example.com");
    }
}
