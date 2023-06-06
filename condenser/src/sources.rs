use crate::{advisors, config, errors, processing::Sourceable, utils, wikidata::ItemExt};

/// Holds all the supplementary source data.
pub struct FullSources {
    /// Wikidata data.
    pub wikidata: advisors::WikidataAdvisor,

    /// BCorp data.
    pub bcorp: advisors::BCorpAdvisor,

    /// EU Ecolabel data.
    pub eu_ecolabel: advisors::EuEcolabelAdvisor,

    /// TCO data.
    pub tco: advisors::TcoAdvisor,

    /// Fashion Transparency Index data.
    pub fti: advisors::FashionTransparencyIndexAdvisor,
}

impl FullSources {
    #[allow(clippy::unused_self)]
    pub fn is_product(&self, item: &sustainity_wikidata::data::Item) -> bool {
        item.has_manufacturer()
    }

    pub fn is_organisation(&self, item: &sustainity_wikidata::data::Item) -> bool {
        if self.is_product(item) {
            return false;
        }

        if item.is_organisation() {
            return true;
        }

        if self.wikidata.has_manufacturer_id(&item.id) {
            return true;
        }

        if self.fti.has_company(&item.id) || self.tco.has_company(&item.id) {
            return true;
        }

        if let Some(websites) = item.get_official_websites() {
            let domains = utils::extract_domains_from_urls(&websites);
            if self.bcorp.has_domains(&domains) {
                return true;
            }
        }

        false
    }
}

impl Sourceable for FullSources {
    type Config = config::SourcesConfig;

    /// Constructs a new `FullSources`.
    fn load(config: &Self::Config) -> Result<Self, errors::ProcessingError> {
        let wikidata = advisors::WikidataAdvisor::load(&config.wikidata_cache_path)?;
        let bcorp = advisors::BCorpAdvisor::load(&config.bcorp_path)?;
        let eu_ecolabel = advisors::EuEcolabelAdvisor::load(
            &config.eu_ecolabel_original_path,
            &config.eu_ecolabel_match_path,
        )?;
        let tco = advisors::TcoAdvisor::load(&config.tco_path)?;
        let fti = advisors::FashionTransparencyIndexAdvisor::load(
            &config.fashion_transparency_index_path,
        )?;

        Ok(Self { wikidata, bcorp, eu_ecolabel, tco, fti })
    }
}
