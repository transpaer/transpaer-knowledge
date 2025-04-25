#![deny(clippy::pedantic)]
#![deny(clippy::unwrap_used)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::missing_errors_doc)]

// TODO: add more structure to the files
mod advisors;
mod analysis;
mod bcorp;
mod cache;
mod categories;
mod coagulate;
mod coagulating;
mod commands;
mod condensing;
mod config;
mod connecting;
mod convert;
mod crystalizing;
mod errors;
mod filtering1;
mod filtering2;
mod oxidation;
mod parallel;
mod runners;
mod sampling;
mod score;
mod sources;
mod substrate;
mod updating;
mod utils;
mod wikidata;

pub use crate::{
    analysis::AnalysisRunner, coagulating::Coagulator, condensing::CondensingRunner,
    config::Config, connecting::ConnectionRunner, crystalizing::Crystalizer,
    errors::ProcessingError, filtering1::FilteringRunner as Filtering1Runner,
    filtering2::FilteringRunner as Filtering2Runner, oxidation::Oxidizer, sampling::SamplingRunner,
    updating::UpdateRunner,
};
