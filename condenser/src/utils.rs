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
    if let Some((host, _path)) = domain.split_once('/') {
        domain = host;
    }
    domain.to_lowercase()
}

/// Checks is the path exists and is a file.
pub fn is_path_ok(path: &std::path::Path) -> bool {
    path.exists() && path.is_file()
}

/// Formats duration to a human-readable format.
pub fn format_elapsed_time(duration: std::time::Duration) -> String {
    let duration = duration.as_secs();
    let seconds = duration % 60;
    let minutes = (duration / 60) % 60;
    let hours = duration / 3600;
    format!("{hours}h {minutes}m {seconds}s")
}

#[cfg(test)]
mod tests {
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
}
