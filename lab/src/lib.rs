// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#![deny(clippy::pedantic)]
#![deny(clippy::unwrap_used)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::missing_errors_doc)]

// TODO: add more structure to the files
mod absorbing;
mod advisors;
mod cache;
mod coagulate;
mod coagulating;
mod commands;
mod condensing;
mod config;
mod connecting;
mod convert;
mod crystalizing;
mod errors;
mod extracting;
mod filtering;
mod oxidation;
mod parallel;
mod runners;
mod sampling;
mod score;
mod substrate;
mod traits;
mod updating;
mod utils;
mod wikidata;

pub use crate::{
    absorbing::Absorber, coagulating::Coagulator, condensing::CondensingRunner, config::Config,
    connecting::ConnectionRunner, crystalizing::Crystalizer, errors::ProcessingError,
    extracting::ExtractingRunner, filtering::FilteringRunner, oxidation::Oxidizer,
    sampling::SamplingRunner, updating::UpdateRunner,
};
