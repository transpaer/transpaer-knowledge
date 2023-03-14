//! Contains code ralated to parsing source data.

use crate::{advisors, cache, config};

use consumers_collecting::errors::IoOrSerdeError;

/// Holds all the source data.
pub struct Sources {
    /// Wikidata cache.
    pub cache: cache::Wikidata,

    /// BCorp data.
    pub bcorp: advisors::BCorpAdvisor,

    /// TCO data.
    pub tco: advisors::TcoAdvisor,

    /// Consumers data.
    pub consumers: advisors::ConsumersAdvisor,
}

impl Sources {
    /// Constructs a new `Sources`.
    pub fn new(config: &config::Config) -> Result<Self, IoOrSerdeError> {
        let cache = cache::Loader::new(config.clone()).load()?;

        let bcorp = advisors::BCorpAdvisor::load(&config.bcorp_path)?;
        let tco = advisors::TcoAdvisor::load(&config.tco_path)?;
        let consumers = advisors::ConsumersAdvisor::load(&config.consumers_path);

        Ok(Self { cache, bcorp, tco, consumers })
    }
}
