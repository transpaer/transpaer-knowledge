#![deny(clippy::pedantic)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

pub mod data {
    pub use sustainity_wikidata::data::{Id as WikiId, StrId as WikiStrId};
}

pub mod errors;

pub mod bcorp;
pub mod eu_ecolabel;
pub mod fashion_transparency_index;
pub mod open_food_facts;
pub mod sustainity;
pub mod tco;
