// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::str::FromStr;

use serde::{Deserialize, Serialize};

use transpaer_api::models as api;
use transpaer_models::{
    ids,
    store::{Organisation, Product},
};

fn hack(link: api::TextSearchLink) -> api::TextSearchLinkHack {
    match link {
        api::TextSearchLink::ProductLink(link) => api::TextSearchLinkHack {
            id: link.id,
            product_id_variant: Some(link.product_id_variant),
            organisation_id_variant: None,
        },
        api::TextSearchLink::OrganisationLink(link) => api::TextSearchLinkHack {
            id: link.id,
            organisation_id_variant: Some(link.organisation_id_variant),
            product_id_variant: None,
        },
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SearchResultId {
    Organisation(String),
    Product(String),
}

/// Represents a search result.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OrganisationSearchResult {
    /// DB entry ID.
    #[serde(rename = "id")]
    pub id: String,

    /// IDs of the organisation.
    #[serde(rename = "ids")]
    pub ids: transpaer_models::store::OrganisationIds,

    /// Product name.
    #[serde(rename = "name")]
    pub name: Option<transpaer_models::store::Text>,
}

impl OrganisationSearchResult {
    pub fn from_db(id: ids::OrganisationId, organisation: Organisation) -> Self {
        Self {
            id: id.to_canonical_string(),
            ids: organisation.ids.clone(),
            name: organisation.names.first().cloned(),
        }
    }

    pub fn convert(self) -> Option<(SearchResultId, api::TextSearchResult)> {
        // TODO: perhaps we can somehow ensure that the code will stop compiling if
        // a new field is added to `ids`.
        let (variant, id) = if let Some(id) = self.ids.vat_ids.first() {
            (api::OrganisationIdVariant::Vat, id.to_canonical_string())
        } else if let Some(id) = self.ids.wiki.first() {
            (api::OrganisationIdVariant::Wiki, id.to_canonical_string())
        } else if let Some(id) = self.ids.domains.first() {
            (api::OrganisationIdVariant::Www, id.clone())
        } else {
            // TODO: perhaps we should default to the database ID
            return None;
        };

        Some((
            SearchResultId::Organisation(self.id),
            api::TextSearchResult {
                link: hack(api::TextSearchLink::OrganisationLink(api::OrganisationLink {
                    organisation_id_variant: variant,
                    id: api::Id::from_str(&id).expect("create ID"),
                })),
                label: api::ShortString::from_str(
                    &self.name.map(|t| t.text.clone()).unwrap_or_default(),
                )
                .expect("create ShortString"),
            },
        ))
    }
}

/// Represents a search result.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProductSearchResult {
    /// DB entry ID.
    #[serde(rename = "id")]
    pub id: String,

    /// IDs of the product.
    #[serde(rename = "ids")]
    pub ids: transpaer_models::store::ProductIds,

    /// Product name.
    #[serde(rename = "name")]
    pub name: Option<transpaer_models::store::Text>,
}

impl ProductSearchResult {
    pub fn from_db(id: ids::ProductId, product: Product) -> Self {
        Self {
            id: id.to_canonical_string(),
            ids: product.ids.clone(),
            name: product.names.first().cloned(),
        }
    }

    pub fn convert(self) -> Option<(SearchResultId, api::TextSearchResult)> {
        // TODO: perhaps we can somehow ensure that the code will stop compiling if
        // a new field is added to `ids`.
        let (variant, id) = if let Some(id) = self.ids.gtins.first() {
            (api::ProductIdVariant::Gtin, id.to_canonical_string())
        } else if let Some(id) = self.ids.eans.first() {
            (api::ProductIdVariant::Ean, id.to_canonical_string())
        } else if let Some(id) = self.ids.wiki.first() {
            (api::ProductIdVariant::Wiki, id.to_canonical_string())
        } else {
            // TODO: perhaps we should default to the database ID
            return None;
        };

        Some((
            SearchResultId::Product(self.id),
            api::TextSearchResult {
                link: hack(api::TextSearchLink::ProductLink(api::ProductLink {
                    product_id_variant: variant,
                    id: api::Id::from_str(&id).expect("create ID"),
                })),
                label: api::ShortString::from_str(
                    &self.name.map(|t| t.text.clone()).unwrap_or_default(),
                )
                .expect("create ShortString"),
            },
        ))
    }
}
