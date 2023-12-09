#![deny(clippy::pedantic)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::missing_errors_doc)]

pub mod advisors;
pub mod analysis;
pub mod cache;
pub mod categories;
pub mod commands;
pub mod condensing;
pub mod config;
pub mod connecting;
pub mod convert;
pub mod errors;
pub mod filtering;
pub mod parallel;
pub mod prefiltering;
pub mod runners;
pub mod score;
pub mod sources;
pub mod transcribing;
pub mod updating;
pub mod utils;
pub mod wikidata;
