// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::{BTreeMap, BTreeSet, HashSet, btree_map::Entry};

use transpaer_collecting::categories::{self, Category};
use transpaer_models::{
    buckets::{Bucket, BucketError, DbStore},
    combine::TryCombine,
    gather, store, transpaer, utils,
};
use transpaer_schema as schema;

use crate::{
    coagulate::{Coagulate, ExternalId, InnerId},
    config,
    errors::{self, CrystalizationError},
    substrate::{DataSetId, Substrate, Substrates},
};

// TODO: Rework as reports per data source
#[allow(clippy::struct_field_names)]
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
                    .map_or_else(|| UNKNOWN.to_string(), |path| format!("{}", path.display()));
                log::warn!("  - `{}`: {}", path, ids.len());
            }
        }
        if !self.empty_ids.is_empty() {
            log::warn!(" empty IDs:");
            for (data_set_id, ids) in &self.empty_ids {
                let path = substrates
                    .get_path_for_id(*data_set_id)
                    .map_or_else(|| UNKNOWN.to_string(), |path| format!("{}", path.display()));
                log::warn!("  - `{}`: {}", path, ids.len());
            }
        }
        if !self.missing_inner_ids.is_empty() {
            log::warn!(" missing inner IDs:");
            for (data_set_id, ids) in &self.missing_inner_ids {
                let path = substrates
                    .get_path_for_id(*data_set_id)
                    .map_or_else(|| UNKNOWN.to_string(), |path| format!("{}", path.display()));
                log::warn!("  - `{}`: {}", path, ids.len());
            }
        }
        log::warn!("End of the report");
    }
}

/// Prepares  the crystalization report fron the collector.
#[derive(Debug)]
pub struct Summary {
    num_products: usize,
    num_products_with_category: usize,
    products_in_category: BTreeMap<String, usize>,
}

impl Summary {
    pub fn create(collector: &CrystalizationCollector) -> Result<Self, BucketError> {
        log::info!("Sumarizing...");

        let mut products_in_category = BTreeMap::new();
        let products = collector.get_product_bucket()?;
        let num_products = products.len();
        let mut num_products_with_category = 0;
        for item in products.iter() {
            let (_, product) = item?;
            if !product.categories.is_empty() {
                num_products_with_category += 1;
            }
            for category in &product.all_categories(categories::SEPARATOR) {
                products_in_category
                    .entry(category.clone())
                    .and_modify(|amount| *amount += 1)
                    .or_insert_with(|| 1);
            }
        }

        Ok(Self { num_products, num_products_with_category, products_in_category })
    }

    pub fn report(&self) {
        log::info!("Summary:");
        log::info!(
            " * {} out of {} products have a category",
            self.num_products_with_category,
            self.num_products
        );
        log::info!(" * products per category:");
        for (category, amount) in &self.products_in_category {
            log::info!("   - {category: <120} {amount: >5}");
        }
    }
}

/// Data storage for gathered data.
///
/// Allows merging different instances.
#[derive(Debug, Clone)]
pub struct CrystalizationCollector {
    /// Stores a list of products and all organisations.
    ///
    /// Since the lists contain several gigabytes of data it's necessary to store them in a database
    /// stored on a disk.
    store: kv::Store,
}

impl CrystalizationCollector {
    pub fn new(path: &std::path::Path) -> Result<Self, BucketError> {
        Ok(Self { store: kv::Store::new(kv::Config::new(path))? })
    }

    pub fn update_organisation(
        &mut self,
        id: &gather::OrganisationId,
        substrate_name: String,
        mut organisation: gather::Organisation,
    ) -> Result<(), errors::CrystalizationError> {
        organisation.transpaer.assign_significance(
            substrate_name,
            transpaer::calculate_organisation_significance(&organisation),
        );
        let orgs = self.get_organisation_bucket()?;
        let org = match orgs.get(id)? {
            Some(org) => TryCombine::try_combine(org, organisation)?,
            None => organisation,
        };
        orgs.insert(id, &org)?;
        Ok(())
    }

    pub fn update_product(
        &mut self,
        id: &gather::ProductId,
        substrate_name: String,
        mut product: gather::Product,
    ) -> Result<(), errors::CrystalizationError> {
        product.transpaer.assign_significance(
            substrate_name,
            transpaer::calculate_product_significance(&product),
        );
        let prods = self.get_product_bucket()?;
        let prod = match prods.get(id)? {
            Some(prod) => TryCombine::try_combine(prod, product)?,
            None => product,
        };
        prods.insert(id, &prod)?;
        Ok(())
    }

    fn get_organisation_bucket(
        &self,
    ) -> Result<Bucket<'_, gather::OrganisationId, gather::Organisation>, BucketError> {
        Bucket::obtain(&self.store, "organisation.id => organisation")
    }

    fn get_product_bucket(
        &self,
    ) -> Result<Bucket<'_, gather::ProductId, gather::Product>, BucketError> {
        Bucket::obtain(&self.store, "product.id => product")
    }
}

#[derive(Debug)]
pub struct Processor {
    /// Collected data.
    collector: CrystalizationCollector,

    /// Report listing warnings from substrate files.
    report: CrystalizationReport,
}

impl Processor {
    pub fn new(runtime_path: &std::path::Path) -> Result<Self, BucketError> {
        Ok(Self {
            collector: CrystalizationCollector::new(runtime_path)?,
            report: CrystalizationReport::default(),
        })
    }

    fn process(
        mut self,
        substrates: &Substrates,
        coagulate: &Coagulate,
    ) -> Result<(CrystalizationCollector, CrystalizationReport), errors::CrystalizationError> {
        log::info!("Processing substrates");
        for substrate in substrates.list() {
            log::info!(" -> {}", substrate.name);
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
            &unique_id,
            substrate.name.clone(),
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
                media: BTreeSet::new(),
                products: BTreeSet::new(), //< filled later
                transpaer: gather::TranspaerOrganisationData::default(),
            },
        )?;

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
            &unique_id,
            substrate.name.clone(),
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
                    c.categories.iter().map(|c| c.0.clone()).collect()
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
                shopping: product.shopping.map_or_else(BTreeSet::new, |shopping| {
                    shopping.iter().map(gather::ShoppingEntry::from_schema).collect()
                }),
                media: BTreeSet::new(),
                follows,
                followed_by,
                certifications: gather::Certifications::default(),
                transpaer: gather::TranspaerProductData::default(), //< Calculated later
            },
        )?;

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
            &unique_id,
            substrate.name.clone(),
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
                categories: product.categorisation.categories.iter().map(|c| c.0.clone()).collect(),
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
                shopping: product.shopping.map_or_else(BTreeSet::new, |shopping| {
                    shopping.iter().map(gather::ShoppingEntry::from_schema).collect()
                }),
                media: BTreeSet::new(),
                follows,
                followed_by,
                certifications: gather::Certifications::default(),
                transpaer: gather::TranspaerProductData::default(), //< Calculated later
            },
        )?;

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
            &unique_id,
            substrate.name.clone(),
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
                media: Self::extract_media_mentions(producer.reports.as_ref(), &substrate.source),
                certifications,
                products: BTreeSet::new(), //< filled later
                transpaer: gather::TranspaerOrganisationData::default(),
            },
        )?;

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
            &unique_id,
            substrate.name.clone(),
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
                    c.categories.iter().map(|c| c.0.clone()).collect()
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
                shopping: product.shopping.map_or_else(BTreeSet::new, |shopping| {
                    shopping.iter().map(gather::ShoppingEntry::from_schema).collect()
                }),
                media: Self::extract_media_mentions(product.reports.as_ref(), &substrate.source),
                follows,
                followed_by,
                certifications: gather::Certifications::default(), //< Assigned later from producers
                transpaer: gather::TranspaerProductData::default(), //< Calculated later
            },
        )?;

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

    fn extract_media_mentions(
        reports: Option<&schema::Reports>,
        source: &gather::Source,
    ) -> BTreeSet<gather::Medium> {
        if let Some(reports) = reports {
            let source = gather::MentionSource::from(source);
            let mut mentions = Vec::new();
            for report in &reports.0 {
                if let Some(url) = &report.url
                    && utils::extract_domain_from_url(url) == "youtube.com"
                {
                    mentions.push(gather::Mention {
                        title: report.title.as_ref().unwrap_or(url).clone(),
                        link: url.clone(),
                    });
                }
            }
            if mentions.is_empty() {
                BTreeSet::new()
            } else {
                maplit::btreeset! {gather::Medium { source, mentions }}
            }
        } else {
            BTreeSet::new()
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
            // We know in BCorp data there is always only one report.
            report_url: producer
                .reports
                .as_ref()?
                .0
                .first()
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

    fn finalize<'a>(
        organisations: &'a mut Bucket<'a, gather::OrganisationId, gather::Organisation>,
        products: &Bucket<gather::ProductId, gather::Product>,
    ) -> Result<(), CrystalizationError> {
        log::info!("Finalizing products");

        // Assign
        //  - certifications to products
        //  - product to organisations
        log::info!(" -> assigning certifications");
        for product in products.clone().iter_autosave() {
            let mut product = product?;
            for manufacturer_id in &product.value.manufacturers {
                if let Some(mut organisation) = organisations.edit(manufacturer_id.clone())? {
                    product
                        .value
                        .certifications
                        .inherit(&organisation.value.certifications.clone());
                    organisation.value.products.insert(product.key.clone());
                }

                // TODO: There are many organisations that cannot be found.
                //       It seems like all of them are bugs in Wikidata.
                //       Make sure all organisations are found.
            }
        }

        // Calculate product Transpaer score
        log::info!(" -> calculating Transpaer scores");
        for product in products.clone().iter_autosave() {
            let mut product = product?;
            product.value.transpaer.score = crate::score::calculate(&product.value);
        }

        Ok(())
    }

    fn convert_category_status(
        status: transpaer_collecting::categories::Status,
    ) -> store::CategoryStatus {
        use transpaer_collecting::categories::Status;
        match status {
            Status::Exploratory => store::CategoryStatus::Exploratory,
            Status::Incomplete => store::CategoryStatus::Incomplete,
            Status::Satisfactory => store::CategoryStatus::Satisfactory,
            Status::Complete => store::CategoryStatus::Complete,
            Status::Broad => store::CategoryStatus::Broad,
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
        organisations: &mut Bucket<gather::OrganisationId, gather::Organisation>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "organisation.id => organisation";
        log::info!(" -> `{COMMENT}`");

        let bucket = self.store.get_organisation_bucket()?;
        for iter in organisations.iter() {
            let (id, org) = iter?;
            bucket.insert(&id, &org.store())?;
        }

        Ok(())
    }

    /// Stores organsation keywords data.
    ///
    /// This data is needed to implement an efficient text search index.
    fn store_organisation_keywords(
        &self,
        organisations: &mut Bucket<gather::OrganisationId, gather::Organisation>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "keywords => [organisation.id]";
        log::info!(" -> `{COMMENT}`");

        let mut data = BTreeMap::<String, Vec<store::OrganisationId>>::new();
        for item in organisations.iter() {
            let (organisation_id, organisation) = item?;
            for keyword in Self::extract_keywords(&organisation.names) {
                data.entry(keyword)
                    .and_modify(|ids| ids.push(organisation_id.clone()))
                    .or_insert_with(|| vec![organisation_id.clone()]);
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
        organisations: &mut Bucket<gather::OrganisationId, gather::Organisation>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "organisation.vat_id => organisation.id";

        log::info!(" -> `{COMMENT}`");

        let bucket = self.store.get_vat_id_to_organisation_id_bucket()?;

        let mut uniqueness_check = HashSet::new();
        for item in organisations.iter() {
            let (organisation_id, organisation) = item?;
            for vat_id in organisation.ids.vat_ids {
                bucket.insert(&vat_id, &organisation_id)?;
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
        organisations: &mut Bucket<gather::OrganisationId, gather::Organisation>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "organisation.wiki_id => organisation.id";

        log::info!(" -> `{COMMENT}`");

        let bucket = self.store.get_wiki_id_to_organisation_id_bucket()?;

        let mut uniqueness_check = HashSet::new();
        for item in organisations.iter() {
            let (organisation_id, organisation) = item?;
            for wiki_id in organisation.ids.wiki {
                bucket.insert(&wiki_id, &organisation_id)?;
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
        organisations: &mut Bucket<gather::OrganisationId, gather::Organisation>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "organisation.WWW_domain => organisation.id";

        log::info!(" -> `{COMMENT}`");

        let bucket = self.store.get_www_domain_to_organisation_id_bucket()?;

        let mut uniqueness_check = HashSet::new();
        for item in organisations.iter() {
            let (organisation_id, organisation) = item?;
            for domain in organisation.ids.domains {
                bucket.insert(&domain, &organisation_id)?;
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
        products: &mut Bucket<gather::ProductId, gather::Product>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "product.id => product";
        log::info!(" -> `{COMMENT}`");

        let bucket = self.store.get_product_bucket()?;
        for item in products.iter() {
            let (product_id, product) = item?;
            let product = product.store();
            bucket.insert(&product_id, &product)?;

            // Make sure that the DB can be deserialized
            assert!(bucket.get(&product_id).is_ok(), "DB integrity: {product_id:?} => {product:?}");
        }

        bucket.flush()?;
        Ok(())
    }

    /// Stores product keywords data.
    ///
    /// This data is needed to implement an efficient text search index.
    fn store_product_keywords(
        &self,
        products: &mut Bucket<gather::ProductId, gather::Product>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "keywords => [product.id]";
        log::info!(" -> `{COMMENT}`");

        let mut data = BTreeMap::<String, Vec<store::ProductId>>::new();
        for item in products.iter() {
            let (product_id, product) = item?;
            for keyword in Self::extract_keywords(&product.names) {
                data.entry(keyword)
                    .and_modify(|ids| ids.push(product_id.clone()))
                    .or_insert_with(|| vec![product_id.clone()]);
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
        products: &mut Bucket<gather::ProductId, gather::Product>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "product.ean => product.id";

        log::info!(" -> `{COMMENT}`");

        let bucket = self.store.get_ean_to_product_id_bucket()?;

        let mut uniqueness_check = HashSet::new();
        for item in products.iter() {
            let (product_id, product) = item?;
            for ean in product.ids.eans {
                bucket.insert(&ean, &product_id)?;
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
        products: &mut Bucket<gather::ProductId, gather::Product>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "product.gtin => product.id";

        log::info!(" -> `{COMMENT}`");

        let bucket = self.store.get_gtin_to_product_id_bucket()?;

        let mut uniqueness_check = HashSet::new();
        for item in products.iter() {
            let (product_id, product) = item?;
            for gtin in product.ids.gtins {
                bucket.insert(&gtin, &product_id)?;
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
        products: &mut Bucket<gather::ProductId, gather::Product>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "product.wiki_id => product.id";

        log::info!(" -> `{COMMENT}`");

        let bucket = self.store.get_wiki_id_to_product_id_bucket()?;

        let mut uniqueness_check = HashSet::new();
        for item in products.iter() {
            let (product_id, product) = item?;
            for wiki_id in product.ids.wiki {
                bucket.insert(&wiki_id, &product_id)?;
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
        products: &mut Bucket<gather::ProductId, gather::Product>,
    ) -> Result<(), errors::CrystalizationError> {
        const COMMENT: &str = "product.category => [product.id]";

        log::info!(" -> `{COMMENT}`");

        let mut data = BTreeMap::<String, Vec<store::ProductId>>::new();
        for item in products.iter() {
            let (product_id, product) = item?;
            for category in product.all_categories(categories::SEPARATOR) {
                data.entry(category.clone())
                    .and_modify(|ids| ids.push(product_id.clone()))
                    .or_insert_with(|| vec![product_id.clone()]);
            }
        }

        let bucket = self.store.get_categories_bucket()?;

        #[allow(clippy::unwrap_used)]
        let info = Category::new(String::new())
            .expect("root category must exist")
            .get_info()
            .expect("root category must exist");
        bucket.insert(
            &String::new(),
            &store::Category {
                status: store::CategoryStatus::Broad,
                subcategories: info.subcategories,
                products: None,
            },
        )?;

        for (category_name, ids) in data {
            #[allow(clippy::unwrap_used)]
            let info = Category::new(category_name.clone())
                .expect("all categories should be valid at this point")
                .get_info()
                .expect("all categories should be valid at this point");

            let product_ids = if info.status.are_products_comparable() { Some(ids) } else { None };

            let category = store::Category {
                status: Self::convert_category_status(info.status),
                subcategories: info.subcategories,
                products: product_ids,
            };

            bucket.insert(&category_name, &category)?;
        }

        bucket.flush()?;
        Ok(())
    }

    fn store_all(self, collector: &CrystalizationCollector) -> Result<(), errors::ProcessingError> {
        Self::finalize(
            &mut collector.get_organisation_bucket()?,
            &collector.get_product_bucket()?,
        )?;

        self.store_organisation_keywords(&mut collector.get_organisation_bucket()?)?;
        self.store_organisation_vat_ids(&mut collector.get_organisation_bucket()?)?;
        self.store_organisation_wiki_ids(&mut collector.get_organisation_bucket()?)?;
        self.store_organisation_www_domains(&mut collector.get_organisation_bucket()?)?;
        self.store_organisations(&mut collector.get_organisation_bucket()?)?;

        self.store_product_keywords(&mut collector.get_product_bucket()?)?;
        self.store_product_eans(&mut collector.get_product_bucket()?)?;
        self.store_product_gtins(&mut collector.get_product_bucket()?)?;
        self.store_product_wiki_ids(&mut collector.get_product_bucket()?)?;
        self.store_categories(&mut collector.get_product_bucket()?)?;
        self.store_products(&mut collector.get_product_bucket()?)?;

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
                Processor::new(&config.runtime)?.process(&substrates, &coagulate)?;
            crystalizer_report.report(&substrates);
            Summary::create(&collector)?.report();

            let store = DbStore::new(&config.crystal)?;
            Saver::new(store).store_all(&collector)?;
            Ok(())
        })
    }
}
