// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::{HashMap, hash_map::Entry};

use crate::{
    gather::{Organisation, Product},
    models::{Significance, Source},
};

pub fn calculate_product_significances(product: &Product) -> HashMap<Source, Significance> {
    const GTIN: f64 = 100.0;
    const WIKIDATA_ID: f64 = 10.0;
    const NAME: f64 = 1.0;
    const DESCRIPTION: f64 = 1.0;
    const IMAGES: f64 = 1.0;
    const ORIGINS: f64 = 50.0;
    const MEDIA: f64 = 50.0;

    // TODO: Add categories, regions, manufacturers and shopping.
    // const CATEGORIES: f64 = 100.0;
    // const REGIONS: f64 = 50.0;
    // const MANUFACTURERS: f64 = 10.0;
    // const SHOPPING: f64 = 100.0;

    let mut result = HashMap::<Source, Significance>::new();

    let mut update_one = |source: Source, value: f64| match result.entry(source) {
        Entry::Vacant(entry) => {
            entry.insert(Significance::new(value));
        }
        Entry::Occupied(mut entry) => {
            entry.get_mut().add(value);
        }
    };

    let mut update = |sources: &[Source], value: f64| {
        for source in sources {
            update_one(source.clone(), value);
        }
    };

    update(&product.ids.eans.collect_sources(), GTIN);
    update(&product.ids.gtins.collect_sources(), GTIN);
    update(&product.ids.wiki.collect_sources(), WIKIDATA_ID);
    update(&product.names.collect_sources(), NAME);
    update(&product.descriptions.collect_sources(), DESCRIPTION);
    update(&product.origins.collect_sources(), ORIGINS);

    for image in &product.images {
        update_one(image.source.clone(), IMAGES);
    }
    for medium in &product.media {
        update_one(medium.source.clone(), MEDIA);
    }

    result
}

pub fn calculate_organisation_significances(
    organisation: &Organisation,
) -> HashMap<Source, Significance> {
    const VAT_ID: f64 = 100.0;
    const WIKIDATA_ID: f64 = 10.0;
    const DOMAIN: f64 = 10.0;
    const NAME: f64 = 1.0;
    const DESCRIPTION: f64 = 1.0;
    const IMAGE: f64 = 1.0;
    const WEBSITE: f64 = 1.0;
    const ORIGIN: f64 = 50.0;
    const MEDIA: f64 = 50.0;

    let mut result = HashMap::<Source, Significance>::new();

    let mut update_one = |source: Source, value: f64| match result.entry(source) {
        Entry::Vacant(entry) => {
            entry.insert(Significance::new(value));
        }
        Entry::Occupied(mut entry) => {
            entry.get_mut().add(value);
        }
    };

    let mut update = |sources: &[Source], value: f64| {
        for source in sources {
            update_one(source.clone(), value);
        }
    };

    update(&organisation.ids.vat_ids.collect_sources(), VAT_ID);
    update(&organisation.ids.wiki.collect_sources(), WIKIDATA_ID);
    update(&organisation.ids.domains.collect_sources(), DOMAIN);
    update(&organisation.names.collect_sources(), NAME);
    update(&organisation.descriptions.collect_sources(), DESCRIPTION);
    update(&organisation.websites.collect_sources(), WEBSITE);
    update(&organisation.origins.collect_sources(), ORIGIN);

    for image in &organisation.images {
        update_one(image.source.clone(), IMAGE);
    }
    for medium in &organisation.media {
        update_one(medium.source.clone(), MEDIA);
    }

    result
}
