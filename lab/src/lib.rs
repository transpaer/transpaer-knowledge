#![deny(clippy::pedantic)]
#![deny(clippy::unwrap_used)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::missing_errors_doc)]

// TODO: add more structure to the files
pub mod advisors;
pub mod analysis;
pub mod bcorp;
pub mod cache;
pub mod categories;
pub mod commands;
pub mod condensing;
pub mod config;
pub mod connecting;
pub mod convert;
pub mod crystalizing;
pub mod errors;
pub mod filtering1;
pub mod filtering2;
pub mod oxidation;
pub mod parallel;
pub mod runners;
pub mod sampling;
pub mod score;
pub mod sources;
pub mod updating;
pub mod utils;
pub mod wikidata;
