#![deny(clippy::pedantic)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![allow(clippy::module_name_repetitions)]

pub mod advisors;
pub mod analysis;
pub mod cache;
pub mod categories;
pub mod condensing;
pub mod config;
pub mod connecting;
pub mod errors;
pub mod filtering;
pub mod future_pool;
pub mod knowledge;
pub mod prefiltering;
pub mod processing;
pub mod runners;
pub mod score;
pub mod sources;
pub mod transcribing;
pub mod updating;
pub mod utils;
pub mod wikidata;
