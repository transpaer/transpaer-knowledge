// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#![deny(clippy::pedantic)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

pub mod data {
    pub use transpaer_wikidata::data::{Id as WikiId, StrId as WikiStrId};
}

pub mod errors;

pub mod categories;
pub mod fetch_info;

pub mod bcorp;
pub mod eu_ecolabel;
pub mod fashion_transparency_index;
pub mod open_food_facts;
pub mod tco;
pub mod transpaer;
