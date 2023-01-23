//! Contains code ralated to parsing source data.

use std::collections::HashSet;

use consumers_collecting::bcorp;

use crate::utils;

/// Holds the information read from the BCorp data.
pub struct BCorpAdvisor {
    /// Domains of BCorp companies.
    domains: HashSet<String>,
}

impl BCorpAdvisor {
    /// Constructs a new `BCorpAdvisor`.
    pub fn new(records: &Vec<bcorp::data::Record>) -> Self {
        let domains: HashSet<String> =
            records.iter().map(|r| utils::extract_domain_from_url(&r.website)).collect();
        Self { domains }
    }

    /// Checks if at least one of the passed domains corresponds to a BCorp company.
    pub fn has_domains(&self, domains: &HashSet<String>) -> bool {
        for domain in domains {
            if self.domains.contains(domain) {
                return true;
            }
        }
        false
    }
}

/// Holds the information read from out internal data set.
pub struct ConsumersAdvisor {}

impl ConsumersAdvisor {
    /// Loads th data from a file.
    pub fn load<P: AsRef<std::path::Path>>(_path: P) -> Result<Self, ()> {
        Ok(Self {})
    }
}
