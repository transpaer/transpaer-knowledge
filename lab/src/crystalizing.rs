use std::collections::{btree_map::Entry, BTreeMap, BTreeSet, HashSet};

use merge::Merge;

use sustainity_models::{
    buckets::{Bucket, DbStore},
    gather, store,
};
use sustainity_schema as schema;

use crate::{
    coagulate::{Coagulate, ExternalId, InnerId},
    config, errors,
    substrate::{DataSetId, Substrate, Substrates},
};

const MAX_CATEGORY_PRODUCT_NUM: usize = 300_000;

// TODO: Rework as repotts per data source
#[must_use]
#[derive(Debug, Default)]
pub struct CrystalizationReport {
    invalid_ids: BTreeMap<DataSetId, BTreeSet<String>>,
    empty_ids: BTreeMap<DataSetId, BTreeSet<InnerId>>,
    missing_inner_ids: BTreeMap<DataSetId, BTreeSet<InnerId>>,
}

impl CrystalizationReport {
    pub fn add_invalid_id(&mut self, data_set_id: DataSetId, id: String) {
        match self.invalid_ids.entry(data_set_id) {
            Entry::Occupied(mut e) => {
                e.get_mut().insert(id);
            }
            Entry::Vacant(e) => {
                let mut set = BTreeSet::new();
                set.insert(id);
                e.insert(set);
            }
        }
    }

    pub fn add_missing_external_id(&mut self, external_id: ExternalId) {
        let (data_set, inner) = external_id.decompose();
        self.add_missing_inner_id(data_set, inner);
    }

    pub fn add_missing_inner_id(&mut self, data_set_id: DataSetId, inner_id: InnerId) {
        match self.missing_inner_ids.entry(data_set_id) {
            Entry::Occupied(mut e) => {
                e.get_mut().insert(inner_id);
            }
            Entry::Vacant(e) => {
                let mut set = BTreeSet::new();
                set.insert(inner_id);
                e.insert(set);
            }
        }
    }

    pub fn report(&self, substrates: &Substrates) {
        const UNKNOWN: &str = "unknown";

        log::warn!("Crystalisation report:");

        if !self.invalid_ids.is_empty() {
            log::warn!(" invalid IDs:");
            for (data_set_id, ids) in &self.invalid_ids {
                let path = substrates
                    .get_path_for_id(*data_set_id)
                    .map_or_else(|| UNKNOWN.to_string(), |path| format!("{path:?}"));
                log::warn!("  - {}: {}", path, ids.len());
            }
        }
        if !self.empty_ids.is_empty() {
            log::warn!(" empty IDs:");
            for (data_set_id, ids) in &self.empty_ids {
                let path = substrates
                    .get_path_for_id(*data_set_id)
                    .map_or_else(|| UNKNOWN.to_string(), |path| format!("{path:?}"));
                log::warn!("  - {}: {}", path, ids.len());
            }
        }
        if !self.missing_inner_ids.is_empty() {
            log::warn!(" missing inner IDs:");
            for (data_set_id, ids) in &self.missing_inner_ids {
                let path = substrates
                    .get_path_for_id(*data_set_id)
                    .map_or_else(|| UNKNOWN.to_string(), |path| format!("{path:?}"));
                log::warn!("  - {}: {}", path, ids.len());
            }
        }
        log::warn!("End of the report");
    }
}

/// Data storage for gathered data.
///
/// Allows merging different instances.
#[derive(Debug, Default, Clone)]
pub struct CrystalizationCollector {
    /// Found organisations.
    organisations: BTreeMap<gather::OrganisationId, gather::Organisation>,

    /// Found products.
    products: BTreeMap<gather::ProductId, gather::Product>,
}

impl CrystalizationCollector {
    pub fn update_organisation(
        &mut self,
        id: gather::OrganisationId,
        organisation: gather::Organisation,
    ) {
        match self.organisations.entry(id) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().merge(organisation);
            }
            Entry::Vacant(entry) => {
                entry.insert(organisation);
            }
        }
    }

    pub fn update_product(&mut self, id: gather::ProductId, product: gather::Product) {
        match self.products.entry(id) {
            Entry::Occupied(mut entry) => entry.get_mut().merge(product),
            Entry::Vacant(entry) => {
                let _ = entry.insert(product);
            }
        }
    }
}

#[derive(Debug, derive_new::new)]
pub struct Processor {
    /// Collected data.
    #[new(default)]
    collector: CrystalizationCollector,

    /// Report listing warnings from substrate files.
    #[new(default)]
    report: CrystalizationReport,
}

impl Processor {
    fn process(
        mut self,
        substrates: &Substrates,
        coagulate: &Coagulate,
    ) -> Result<(CrystalizationCollector, CrystalizationReport), errors::CrystalizationError> {
        log::info!("Processing data");
        for substrate in substrates.list() {
            match schema::read::iter_file(&substrate.path)? {
                schema::read::FileIterVariant::Catalog(iter) => {
                    for entry in iter {
                        match entry? {
                            schema::CatalogEntry::Producer(producer) => {
                                self.process_catalog_producer(producer, substrate, coagulate)?;
                            }
                            schema::CatalogEntry::Product(product) => {
                                self.process_catalog_product(product, substrate, coagulate)?;
                            }
                        }
                    }
                }
                schema::read::FileIterVariant::Producer(iter) => {
                    for entry in iter {
                        match entry? {
                            schema::ProducerEntry::Product(product) => {
                                self.process_producer_product(product, substrate, coagulate)?;
                            }
                            schema::ProducerEntry::Reviewer(_reviewer) => {
                                // TODO: use the reviewer data
                            }
                        }
                    }
                }
                schema::read::FileIterVariant::Review(iter) => {
                    for entry in iter {
                        match entry? {
                            schema::ReviewEntry::Producer(producer) => {
                                self.process_review_producer(producer, substrate, coagulate)?;
                            }
                            schema::ReviewEntry::Product(product) => {
                                self.process_review_product(product, substrate, coagulate)?;
                            }
                        }
                    }
                }
            }
        }
        Ok((self.collector, self.report))
    }

    fn process_catalog_producer(
        &mut self,
        producer: schema::CatalogProducer,
        substrate: &Substrate,
        coagulate: &Coagulate,
    ) -> Result<(), errors::CrystalizationError> {
        let external_id = ExternalId::new(substrate.id, InnerId::new(producer.id));
        let unique_id = coagulate
            .get_unique_id_for_producer_external_id(&external_id)
            .map_err(|id| id.to_error_not_found(substrate, "processing catalog producer"))?;
        let ids = self.convert_organisation_ids(producer.ids, substrate);

        self.collector.update_organisation(
            unique_id.clone(),
            gather::Organisation {
                ids,
                names: producer
                    .names
                    .into_iter()
                    .map(|text| gather::Text { text, source: substrate.source.clone() })
                    .collect(),
                descriptions: producer
                    .description
                    .into_iter()
                    .map(|text| gather::Text { text, source: substrate.source.clone() })
                    .collect(),
                images: producer
                    .images
                    .into_iter()
                    .map(|image| gather::Image { image, source: substrate.source.clone() })
                    .collect(),
                websites: producer.websites.into_iter().collect(),
                origins: Self::extract_producer_origins(producer.origins.as_ref()).map_err(
                    |source| errors::CrystalizationError::IsoCountry {
                        source,
                        when: "processing catalogue producer",
                    },
                )?,
                certifications: gather::Certifications::default(),
                products: BTreeSet::new(), //< filled later
            },
        );

        Ok(())
    }

    fn process_catalog_product(
        &mut self,
        product: schema::CatalogProduct,
        substrate: &Substrate,
        coagulate: &Coagulate,
    ) -> Result<(), errors::CrystalizationError> {
        let external_id = ExternalId::new(substrate.id, InnerId::new(product.id));
        let unique_id = coagulate
            .get_unique_id_for_product_external_id(&external_id)
            .map_err(|id| id.to_error_not_found(substrate, "processing catalog product"))?;
        let ids = self.convert_product_ids(product.ids, substrate);
        let (followed_by, follows) =
            self.extract_related_products(product.related.as_ref(), substrate, coagulate);
        let manufacturers =
            self.extract_manufacturer_ids(product.origins.as_ref(), substrate, coagulate);

        self.collector.update_product(
            unique_id,
            gather::Product {
                ids,
                names: product
                    .names
                    .into_iter()
                    .map(|text| gather::Text { text, source: substrate.source.clone() })
                    .collect(),
                descriptions: product
                    .description
                    .into_iter()
                    .map(|text| gather::Text { text, source: substrate.source.clone() })
                    .collect(),
                images: product
                    .images
                    .into_iter()
                    .map(|image| gather::Image { image, source: substrate.source.clone() })
                    .collect(),
                categories: product.categorisation.map_or_else(BTreeSet::new, |c| {
                    c.categories.iter().map(|c| c.join("/")).collect()
                }),
                regions: Self::extract_regions(product.availability.as_ref()).map_err(
                    |source| errors::CrystalizationError::IsoCountry {
                        source,
                        when: "processing catalogue product regions",
                    },
                )?,
                origins: Self::extract_product_origins(product.origins.as_ref()).map_err(
                    |source| errors::CrystalizationError::IsoCountry {
                        source,
                        when: "processing catalogue product origins",
                    },
                )?,
                manufacturers,
                follows,
                followed_by,
                sustainity_score: gather::SustainityScore::default(), //< Calculated later
                certifications: gather::Certifications::default(),
            },
        );

        Ok(())
    }

    fn process_producer_product(
        &mut self,
        product: schema::ProducerProduct,
        substrate: &Substrate,
        coagulate: &Coagulate,
    ) -> Result<(), errors::CrystalizationError> {
        let external_id = ExternalId::new(substrate.id, InnerId::new(product.id));
        let unique_id = coagulate
            .get_unique_id_for_product_external_id(&external_id)
            .map_err(|id| id.to_error_not_found(substrate, "processing producer product"))?;
        let ids = self.convert_product_ids(product.ids, substrate);
        let (followed_by, follows) =
            self.extract_related_products(product.related.as_ref(), substrate, coagulate);
        let manufacturers =
            self.extract_manufacturer_ids(product.origins.as_ref(), substrate, coagulate);

        self.collector.update_product(
            unique_id,
            gather::Product {
                ids,
                names: product
                    .names
                    .into_iter()
                    .map(|text| gather::Text { text, source: substrate.source.clone() })
                    .collect(),
                descriptions: BTreeSet::new(),
                images: product
                    .images
                    .into_iter()
                    .map(|image| gather::Image { image, source: substrate.source.clone() })
                    .collect(),
                categories: product.categorisation.categories.iter().map(|c| c.join("/")).collect(),
                regions: Self::extract_regions(product.availability.as_ref()).map_err(
                    |source| errors::CrystalizationError::IsoCountry {
                        source,
                        when: "processing producer product regions",
                    },
                )?,
                origins: Self::extract_product_origins(product.origins.as_ref()).map_err(
                    |source| errors::CrystalizationError::IsoCountry {
                        source,
                        when: "processing producer product origins",
                    },
                )?,
                manufacturers,
                follows,
                followed_by,
                sustainity_score: gather::SustainityScore::default(), //< Calculated later
                certifications: gather::Certifications::default(),
            },
        );

        Ok(())
    }

    fn process_review_producer(
        &mut self,
        producer: schema::ReviewProducer,
        substrate: &Substrate,
        coagulate: &Coagulate,
    ) -> Result<(), errors::CrystalizationError> {
        let certifications = gather::Certifications {
            bcorp: Self::extract_bcorp_cert(&producer, substrate),
            eu_ecolabel: Self::extract_euecolabel_cert(substrate),
            fti: Self::extract_fti_cert(&producer, substrate),
            tco: Self::extract_tco_cert(&producer, substrate),
        };

        let external_id = ExternalId::new(substrate.id, InnerId::new(producer.id.clone()));
        let unique_id = coagulate
            .get_unique_id_for_producer_external_id(&external_id)
            .map_err(|id| id.to_error_not_found(substrate, "processing review producer"))?;
        let ids = self.convert_organisation_ids(producer.ids, substrate);

        self.collector.update_organisation(
            unique_id.clone(),
            gather::Organisation {
                ids,
                names: producer
                    .names
                    .into_iter()
                    .map(|text| gather::Text { text, source: substrate.source.clone() })
                    .collect(),
                descriptions: producer
                    .description
                    .into_iter()
                    .map(|text| gather::Text { text, source: substrate.source.clone() })
                    .collect(),
                images: producer
                    .images
                    .into_iter()
                    .map(|image| gather::Image { image, source: substrate.source.clone() })
                    .collect(),
                websites: producer.websites.into_iter().collect(),
                origins: Self::extract_producer_origins(producer.origins.as_ref()).map_err(
                    |source| errors::CrystalizationError::IsoCountry {
                        source,
                        when: "processing review producer",
                    },
                )?,
                certifications,
                products: BTreeSet::new(), //< filled later
            },
        );

        Ok(())
    }

    fn process_review_product(
        &mut self,
        product: schema::ReviewProduct,
        substrate: &Substrate,
        coagulate: &Coagulate,
    ) -> Result<(), errors::CrystalizationError> {
        let external_id = ExternalId::new(substrate.id, InnerId::new(product.id));
        let unique_id = coagulate
            .get_unique_id_for_product_external_id(&external_id)
            .map_err(|id| id.to_error_not_found(substrate, "processing review product"))?;
        let ids = self.convert_product_ids(product.ids, substrate);
        let (followed_by, follows) =
            self.extract_related_products(product.related.as_ref(), substrate, coagulate);
        let manufacturers =
            self.extract_manufacturer_ids(product.origins.as_ref(), substrate, coagulate);

        self.collector.update_product(
            unique_id.clone(),
            gather::Product {
                ids,
                names: product
                    .names
                    .into_iter()
                    .map(|text| gather::Text { text, source: substrate.source.clone() })
                    .collect(),
                descriptions: BTreeSet::new(),
                images: product
                    .images
                    .into_iter()
                    .map(|image| gather::Image { image, source: substrate.source.clone() })
                    .collect(),
                categories: product.categorisation.map_or_else(BTreeSet::new, |c| {
                    c.categories.iter().map(|c| c.join("/")).collect()
                }),
                regions: Self::extract_regions(product.availability.as_ref()).map_err(
                    |source| errors::CrystalizationError::IsoCountry {
                        source,
                        when: "processing review product regions",
                    },
                )?,
                origins: Self::extract_product_origins(product.origins.as_ref()).map_err(
                    |source| errors::CrystalizationError::IsoCountry {
                        source,
                        when: "processing review product origins",
                    },
                )?,
                manufacturers,
                follows,
                followed_by,
                sustainity_score: gather::SustainityScore::default(), //< Calculated later
                certifications: gather::Certifications::default(), //< Assigned later from producers
            },
        );

        Ok(())
    }

    #[allow(clippy::unused_self)]
    fn extract_manufacturer_ids(
        &mut self,
        origins: Option<&schema::ProductOrigins>,
        substrate: &Substrate,
        coagulate: &Coagulate,
    ) -> BTreeSet<gather::OrganisationId> {
        let mut manufacturer_ids = BTreeSet::new();
        if let Some(origins) = &origins {
            for producer_id in &origins.producer_ids {
                let external_id = ExternalId::new(substrate.id, InnerId::new(producer_id.clone()));
                match coagulate.get_unique_id_for_producer_external_id(&external_id) {
                    Ok(unique_id) => {
                        manufacturer_ids.insert(unique_id);
                    }
                    Err(external_id) => self.report.add_missing_external_id(external_id),
                }
            }
        }
        manufacturer_ids
    }

    fn extract_related_products(
        &mut self,
        related: Option<&schema::RelatedProducts>,
        substrate: &Substrate,
        coagulate: &Coagulate,
    ) -> (BTreeSet<gather::ProductId>, BTreeSet<gather::ProductId>) {
        let mut follows = BTreeSet::new();
        let mut followed_by = BTreeSet::new();
        if let Some(related) = related {
            if let Some(precedents) = &related.preceded_by {
                follows = self.convert_inner_ids(precedents, substrate, coagulate);
            }
            if let Some(followers) = &related.followed_by {
                followed_by = self.convert_inner_ids(followers, substrate, coagulate);
            }
        }
        (follows, followed_by)
    }
    fn extract_regions(
        availability: Option<&schema::ProductAvailability>,
    ) -> Result<gather::Regions, isocountry::CountryCodeParseErr> {
        if let Some(availability) = availability {
            Self::convert_regions(&availability.regions)
        } else {
            Ok(gather::Regions::Unknown)
        }
    }

    fn convert_regions(
        regions: &schema::Regions,
    ) -> Result<gather::Regions, isocountry::CountryCodeParseErr> {
        Ok(match regions {
            schema::Regions::Variant(variant) => match variant {
                schema::RegionVariant::All => gather::Regions::World,
                schema::RegionVariant::Unknown => gather::Regions::Unknown,
            },
            schema::Regions::List(list) => gather::Regions::List(Self::convert_region_list(list)?),
        })
    }

    fn convert_region_list(
        list: &schema::RegionList,
    ) -> Result<Vec<isocountry::CountryCode>, isocountry::CountryCodeParseErr> {
        let mut regions = Vec::new();
        for region in &list.0 {
            regions.push(isocountry::CountryCode::for_alpha3(region)?);
        }
        Ok(regions)
    }

    fn extract_product_origins(
        origins: Option<&schema::ProductOrigins>,
    ) -> Result<BTreeSet<isocountry::CountryCode>, isocountry::CountryCodeParseErr> {
        if let Some(origins) = origins {
            if let Some(regions) = &origins.regions {
                Ok(Self::convert_region_list(regions)?.into_iter().collect())
            } else {
                Ok(BTreeSet::new())
            }
        } else {
            Ok(BTreeSet::new())
        }
    }

    fn extract_producer_origins(
        origins: Option<&schema::ProducerOrigins>,
    ) -> Result<BTreeSet<isocountry::CountryCode>, isocountry::CountryCodeParseErr> {
        if let Some(origins) = origins {
            if let Some(regions) = &origins.regions {
                Ok(Self::convert_region_list(regions)?.into_iter().collect())
            } else {
                Ok(BTreeSet::new())
            }
        } else {
            Ok(BTreeSet::new())
        }
    }

    fn convert_inner_ids(
        &mut self,
        input: &[String],
        substrate: &Substrate,
        coagulate: &Coagulate,
    ) -> BTreeSet<gather::ProductId> {
        let mut result = BTreeSet::new();
        for product_id in input {
            let external_id = ExternalId::new(substrate.id, InnerId::new(product_id.clone()));
            match coagulate.get_unique_id_for_product_external_id(&external_id) {
                Ok(unique_id) => {
                    result.insert(unique_id);
                }
                Err(external_id) => self.report.add_missing_external_id(external_id),
            }
        }
        result
    }

    fn extract_bcorp_cert(
        producer: &schema::ReviewProducer,
        substrate: &Substrate,
    ) -> Option<gather::BCorpCert> {
        if !substrate.source.is_bcorp() {
            return None;
        }

        Some(gather::BCorpCert {
            id: producer.id.clone(),
            report_url: producer
                .report
                .as_ref()
                .and_then(|report| report.url.clone())
                .unwrap_or_default(),
        })
    }

    fn extract_euecolabel_cert(substrate: &Substrate) -> Option<gather::EuEcolabelCert> {
        if !substrate.source.is_euecolabel() {
            return None;
        }

        Some(gather::EuEcolabelCert {})
    }

    fn extract_fti_cert(
        producer: &schema::ReviewProducer,
        substrate: &Substrate,
    ) -> Option<gather::FtiCert> {
        if !substrate.source.is_fti() {
            return None;
        }

        match &producer.review {
            Some(schema::Review::ScoreReview(review)) => {
                Some(gather::FtiCert { score: review.value })
            }
            _ => None,
        }
    }

    fn extract_tco_cert(
        producer: &schema::ReviewProducer,
        substrate: &Substrate,
    ) -> Option<gather::TcoCert> {
        if !substrate.source.is_tco() {
            return None;
        }

        // TODO: which name to pick?
        producer.names.first().cloned().map(|brand_name| gather::TcoCert { brand_name })
    }

    fn convert_product_ids(
        &mut self,
        ids: schema::ProductIds,
        substrate: &Substrate,
    ) -> gather::ProductIds {
        let mut eans = BTreeSet::<gather::Ean>::new();
        if let Some(ids) = ids.ean {
            for id in ids {
                match gather::Ean::try_from(&id) {
                    Ok(ean) => {
                        eans.insert(ean);
                    }
                    Err(_) => self.report.add_invalid_id(substrate.id, id),
                }
            }
        }

        let mut gtins = BTreeSet::<gather::Gtin>::new();
        if let Some(ids) = ids.gtin {
            for id in ids {
                match gather::Gtin::try_from(&id) {
                    Ok(gtin) => {
                        gtins.insert(gtin);
                    }
                    Err(_) => self.report.add_invalid_id(substrate.id, id),
                }
            }
        }

        let mut wiki = BTreeSet::<gather::WikiId>::new();
        if let Some(ids) = ids.wiki {
            for id in ids {
                match gather::WikiId::try_from(&id) {
                    Ok(wiki_id) => {
                        wiki.insert(wiki_id);
                    }
                    Err(_) => self.report.add_invalid_id(substrate.id, id),
                }
            }
        }

        gather::ProductIds { eans, gtins, wiki }
    }

    fn convert_organisation_ids(
        &mut self,
        ids: schema::ProducerIds,
        substrate: &Substrate,
    ) -> gather::OrganisationIds {
        let mut vat_ids = BTreeSet::<gather::VatId>::new();
        if let Some(ids) = ids.vat {
            for id in ids {
                match gather::VatId::try_from(&id) {
                    Ok(vat) => {
                        vat_ids.insert(vat);
                    }
                    Err(_) => self.report.add_invalid_id(substrate.id, id),
                }
            }
        }

        let mut wiki = BTreeSet::<gather::WikiId>::new();
        if let Some(ids) = ids.wiki {
            for id in ids {
                match gather::WikiId::try_from(&id) {
                    Ok(wiki_id) => {
                        wiki.insert(wiki_id);
                    }
                    Err(_) => self.report.add_invalid_id(substrate.id, id),
                }
            }
        }

        let mut domains = BTreeSet::<gather::Domain>::new();
        if let Some(ids) = ids.domains {
            for domain in ids {
                domains.insert(domain);
            }
        }

        gather::OrganisationIds { vat_ids, wiki, domains }
    }
}

#[derive(Debug, derive_new::new)]
pub struct Saver {
    store: DbStore,
}

impl Saver {
    /// Extracts keywords for DB text search from passed texts.
    fn extract_keywords(texts: &BTreeSet<gather::Text>) -> BTreeSet<String> {
        let mut result = BTreeSet::new();
        for text in texts {
            for word in text.text.split_whitespace() {
                result.insert(word.to_lowercase());
            }
        }
        result.remove("");
        result
    }

    fn finalize(
        organisations: &mut BTreeMap<gather::OrganisationId, gather::Organisation>,
        products: &mut BTreeMap<gather::ProductId, gather::Product>,
    ) {
        log::info!("Finalizing products");

        // Assign
        //  - certifications to products
        //  - product to organisations
        log::info!(" -> assigning certifications");
        for (product_id, product) in products.iter_mut() {
            for manufacturer_id in &product.manufacturers {
                if let Some(organisation) = organisations.get_mut(manufacturer_id) {
                    product.certifications.inherit(&organisation.certifications);
                    organisation.products.insert(product_id.clone());
                }

                // TODO: There are many organisations that cannot be found.
                //       It seems like all of them are bugs in Wikidata.
                //       Make sure all organisations are found.
            }
        }

        // Calculate product Sustainity score
        log::info!(" -> calculating Sustainity scores");
        for product in products.values_mut() {
            product.sustainity_score = crate::score::calculate(product);
        }
    }

    /// Runs a quick sanity check: the `unique` should contain as many elements as `all`.
    fn uniqueness_check<T1, T2, T3>(
        unique: &HashSet<T1>,
        all: &Bucket<T2, T3>,
        comment: &'static str,
    ) -> Result<(), errors::CrystalizationError> {
        if unique.len() == all.len() {
            Ok(())
        } else {
            Err(errors::CrystalizationError::NotUniqueKeys {
                comment: comment.to_string(),
                unique: unique.len(),
                all: all.len(),
            })
        }
    }

    /// Stores organsation data.
    ///
    /// - fills left-over certifications
    /// - converts into a vector
    fn store_organisations(
        &self,
        organisations: BTreeMap<gather::OrganisationId, gather::Organisation>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "organisation.id => organisation";
        log::info!(" -> `{COMMENT}`");

        let bucket = self.store.get_organisation_bucket()?;
        for (id, organisation) in organisations {
            bucket.insert(&id, &organisation.store())?;
        }

        Ok(())
    }

    /// Stores organsation keywords data.
    ///
    /// This data is needed to implement an efficient text search index.
    fn store_organisation_keywords(
        &self,
        organisations: &BTreeMap<gather::OrganisationId, gather::Organisation>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "keywords => [organisation.id]";
        log::info!(" -> `{COMMENT}`");

        let mut data = BTreeMap::<String, Vec<store::OrganisationId>>::new();
        for (unique_id, organisation) in organisations {
            for keyword in Self::extract_keywords(&organisation.names) {
                data.entry(keyword)
                    .and_modify(|ids| ids.push(unique_id.clone()))
                    .or_insert_with(|| vec![unique_id.clone()]);
            }
        }

        let bucket = self.store.get_keyword_to_organisation_ids_bucket()?;
        for (keyword, ids) in data {
            bucket.insert(&keyword, &ids)?;
        }

        bucket.flush()?;
        Ok(())
    }

    /// Stores VAT data.
    ///
    /// This data is needed to implement an efficient VAT search index.
    fn store_organisation_vat_ids(
        &self,
        organisations: &BTreeMap<gather::OrganisationId, gather::Organisation>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "organisation.vat_id => organisation.id";

        log::info!(" -> `{COMMENT}`");

        let bucket = self.store.get_vat_id_to_organisation_id_bucket()?;

        let mut uniqueness_check = HashSet::new();
        for (organisation_id, organisation) in organisations {
            for vat_id in &organisation.ids.vat_ids {
                bucket.insert(vat_id, organisation_id)?;
                uniqueness_check.insert(vat_id);
            }
        }

        // Sanity check: all keys should be unique
        Self::uniqueness_check(&uniqueness_check, &bucket, COMMENT)?;

        bucket.flush()?;
        Ok(())
    }

    /// Stores Wikidata ID data.
    ///
    /// This data is needed to implement an efficient Wikidata ID search index.
    fn store_organisation_wiki_ids(
        &self,
        organisations: &BTreeMap<gather::OrganisationId, gather::Organisation>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "organisation.wiki_id => organisation.id";

        log::info!(" -> `{COMMENT}`");

        let bucket = self.store.get_wiki_id_to_organisation_id_bucket()?;

        let mut uniqueness_check = HashSet::new();
        for (organisation_id, organisation) in organisations {
            for wiki_id in &organisation.ids.wiki {
                bucket.insert(wiki_id, organisation_id)?;
                uniqueness_check.insert(wiki_id);
            }
        }

        // Sanity check: all keys should be unique
        Self::uniqueness_check(&uniqueness_check, &bucket, COMMENT)?;

        bucket.flush()?;
        Ok(())
    }

    /// Stores organisation WWW domain data.
    ///
    /// This data is needed to implement an efficient WWW domain search index.
    fn store_organisation_www_domains(
        &self,
        organisations: &BTreeMap<gather::OrganisationId, gather::Organisation>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "organisation.WWW_domain => organisation.id";

        log::info!(" -> `{COMMENT}`");

        let bucket = self.store.get_www_domain_to_organisation_id_bucket()?;

        let mut uniqueness_check = HashSet::new();
        for (organisation_id, organisation) in organisations {
            for domain in &organisation.ids.domains {
                bucket.insert(domain, organisation_id)?;
                uniqueness_check.insert(domain);
            }
        }

        // Sanity check: all keys should be unique
        Self::uniqueness_check(&uniqueness_check, &bucket, COMMENT)?;

        bucket.flush()?;
        Ok(())
    }

    /// Stores product data.
    fn store_products(
        &self,
        products: BTreeMap<gather::ProductId, gather::Product>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "product.id => product";
        log::info!(" -> `{COMMENT}`");

        let bucket = self.store.get_product_bucket()?;
        for (id, product) in products {
            let product = product.clone().store();
            bucket.insert(&id, &product)?;

            // Make sure that the DB can be deserialized
            assert!(bucket.get(&id).is_ok(), "DB integrity: {id:?} => {product:?}");
        }

        bucket.flush()?;
        Ok(())
    }

    /// Stores product keywords data.
    ///
    /// This data is needed to implement an efficient text search index.
    fn store_product_keywords(
        &self,
        products: &BTreeMap<gather::ProductId, gather::Product>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "keywords => [product.id]";
        log::info!(" -> `{COMMENT}`");

        let mut data = BTreeMap::<String, Vec<store::ProductId>>::new();
        for (unique_id, product) in products {
            for keyword in Self::extract_keywords(&product.names) {
                data.entry(keyword)
                    .and_modify(|ids| ids.push(unique_id.clone()))
                    .or_insert_with(|| vec![unique_id.clone()]);
            }
        }

        let bucket = self.store.get_keyword_to_product_ids_bucket()?;
        for (keyword, ids) in data {
            bucket.insert(&keyword, &ids)?;
        }

        bucket.flush()?;
        Ok(())
    }

    /// Stores EAN data.
    ///
    /// This data is needed to implement an efficient EAN search index.
    fn store_product_eans(
        &self,
        products: &BTreeMap<gather::ProductId, gather::Product>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "product.ean => product.id";

        log::info!(" -> `{COMMENT}`");

        let bucket = self.store.get_ean_to_product_id_bucket()?;

        let mut uniqueness_check = HashSet::new();
        for (product_id, product) in products {
            for ean in &product.ids.eans {
                bucket.insert(ean, product_id)?;
                uniqueness_check.insert(ean);
            }
        }

        // Sanity check: all keys should be unique
        Self::uniqueness_check(&uniqueness_check, &bucket, COMMENT)?;

        bucket.flush()?;
        Ok(())
    }

    /// Stores GTIN data.
    ///
    /// This data is needed to implement an efficient GTIN search index.
    fn store_product_gtins(
        &self,
        products: &BTreeMap<gather::ProductId, gather::Product>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "product.gtin => product.id";

        log::info!(" -> `{COMMENT}`");

        let bucket = self.store.get_gtin_to_product_id_bucket()?;

        let mut uniqueness_check = HashSet::new();
        for (product_id, product) in products {
            for gtin in &product.ids.gtins {
                bucket.insert(gtin, product_id)?;
                uniqueness_check.insert(gtin);
            }
        }

        // Sanity check: all keys should be unique
        Self::uniqueness_check(&uniqueness_check, &bucket, COMMENT)?;

        bucket.flush()?;
        Ok(())
    }

    /// Stores Wikidata ID data.
    ///
    /// This data is needed to implement an efficient Wikidata ID search index.
    /// Data is composed from Wikidata ID vertex collection and edge collection connecting them to products.
    fn store_product_wiki_ids(
        &self,
        products: &BTreeMap<gather::ProductId, gather::Product>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "product.wiki_id => product.id";

        log::info!(" -> `{COMMENT}`");

        let bucket = self.store.get_wiki_id_to_product_id_bucket()?;

        let mut uniqueness_check = HashSet::new();
        for (product_id, product) in products {
            for wiki_id in &product.ids.wiki {
                bucket.insert(wiki_id, product_id)?;
                uniqueness_check.insert(wiki_id);
            }
        }

        // Sanity check: all keys should be unique
        Self::uniqueness_check(&uniqueness_check, &bucket, COMMENT)?;

        bucket.flush()?;
        Ok(())
    }

    /// Stores category data.
    ///
    /// This data is needed to implement an efficient alternative product search index.
    /// Data is composed from category vertex collection and edge collection connecting them to products.
    fn store_categories(
        &self,
        products: &BTreeMap<gather::ProductId, gather::Product>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "product.category => [product.id]";

        log::info!(" -> `{COMMENT}`");

        let mut data = BTreeMap::<String, Vec<store::ProductId>>::new();
        for (unique_id, product) in products {
            for category in &product.categories {
                data.entry(category.clone())
                    .and_modify(|ids| ids.push(unique_id.clone()))
                    .or_insert_with(|| vec![unique_id.clone()]);
            }
        }

        let bucket = self.store.get_categories_bucket()?;
        for (keyword, ids) in data {
            if ids.len() < MAX_CATEGORY_PRODUCT_NUM {
                bucket.insert(&keyword, &ids)?;
            }
        }

        bucket.flush()?;
        Ok(())
    }

    fn store_all(
        self,
        mut collector: CrystalizationCollector,
    ) -> Result<(), errors::ProcessingError> {
        Self::finalize(&mut collector.organisations, &mut collector.products);

        log::info!("Storing:");

        self.store_organisation_keywords(&collector.organisations)?;
        self.store_organisation_vat_ids(&collector.organisations)?;
        self.store_organisation_wiki_ids(&collector.organisations)?;
        self.store_organisation_www_domains(&collector.organisations)?;
        self.store_organisations(collector.organisations)?;

        self.store_product_keywords(&collector.products)?;
        self.store_product_eans(&collector.products)?;
        self.store_product_gtins(&collector.products)?;
        self.store_product_wiki_ids(&collector.products)?;
        self.store_categories(&collector.products)?;
        self.store_products(collector.products)?;

        log::info!("Crystalisation finished");

        Ok(())
    }
}

pub struct Crystalizer;

impl Crystalizer {
    pub fn run(config: &config::CrystalizationConfig) -> Result<(), errors::ProcessingError> {
        futures::executor::block_on(async {
            let (substrates, substrate_report) =
                Substrates::prepare(&config.substrate.substrate_path)?;
            substrate_report.report();

            let coagulate = Coagulate::read(&config.coagulate, &substrates)?;
            let (collector, crystalizer_report) =
                Processor::new().process(&substrates, &coagulate)?;
            crystalizer_report.report(&substrates);

            let store = DbStore::new(&config.crystal)?;
            Saver::new(store).store_all(collector)?;
            Ok(())
        })
    }
}
