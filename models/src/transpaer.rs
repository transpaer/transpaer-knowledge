// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{
    gather::{Organisation, Product},
    models::Significance,
};

pub fn calculate_product_significance(product: &Product) -> Significance {
    const GTIN: f64 = 100.0;
    const WIKIDATA_ID: f64 = 10.0;
    const NAME: f64 = 1.0;
    const DESCRIPTION: f64 = 1.0;
    const IMAGES: f64 = 1.0;
    const CATEGORIES: f64 = 100.0;
    const REGIONS: f64 = 50.0;
    const ORIGINS: f64 = 50.0;
    const MANUFACTURERS: f64 = 10.0;
    const SHOPPING: f64 = 100.0;
    const MEDIA: f64 = 50.0;

    let mut result = 0.0;

    if !product.ids.eans.is_empty() {
        result += GTIN;
    }
    if !product.ids.gtins.is_empty() {
        result += GTIN;
    }
    if !product.ids.wiki.is_empty() {
        result += WIKIDATA_ID;
    }
    if !product.names.is_empty() {
        result += NAME;
    }
    if !product.descriptions.is_empty() {
        result += DESCRIPTION;
    }
    if !product.images.is_empty() {
        result += IMAGES;
    }
    if !product.categories.is_empty() {
        result += CATEGORIES;
    }
    if !product.regions.is_unknown() {
        result += REGIONS;
    }
    if !product.origins.is_empty() {
        result += ORIGINS;
    }
    if !product.manufacturers.is_empty() {
        result += MANUFACTURERS;
    }
    if !product.shopping.is_empty() {
        result += SHOPPING;
    }
    if !product.media.is_empty() {
        result += MEDIA;
    }

    Significance::new(result)
}

pub fn calculate_organisation_significance(organisation: &Organisation) -> Significance {
    const VAT_ID: f64 = 100.0;
    const WIKIDATA_ID: f64 = 10.0;
    const DOMAINS: f64 = 10.0;
    const NAME: f64 = 1.0;
    const DESCRIPTION: f64 = 1.0;
    const IMAGES: f64 = 1.0;
    const WEBSITES: f64 = 1.0;
    const ORIGINS: f64 = 50.0;
    const MEDIA: f64 = 50.0;

    let mut result = 0.0;

    if !organisation.ids.vat_ids.is_empty() {
        result += VAT_ID;
    }
    if !organisation.ids.wiki.is_empty() {
        result += WIKIDATA_ID;
    }
    if !organisation.ids.domains.is_empty() {
        result += DOMAINS;
    }
    if !organisation.names.is_empty() {
        result += NAME;
    }
    if !organisation.descriptions.is_empty() {
        result += DESCRIPTION;
    }
    if !organisation.images.is_empty() {
        result += IMAGES;
    }
    if !organisation.websites.is_empty() {
        result += WEBSITES;
    }
    if !organisation.origins.is_empty() {
        result += ORIGINS;
    }
    if !organisation.media.is_empty() {
        result += MEDIA;
    }

    Significance::new(result)
}
