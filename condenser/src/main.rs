#![deny(clippy::pedantic)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![allow(clippy::module_name_repetitions)]

mod advisors;
mod analysis;
mod cache;
mod categories;
mod condensing;
mod config;
mod connecting;
mod errors;
mod filtering;
mod future_pool;
mod knowledge;
mod prefiltering;
mod processing;
mod runners;
mod score;
mod sources;
mod transcribing;
mod utils;
mod wikidata;

use processing::Runnable;

async fn run() -> Result<(), errors::ProcessingError> {
    match config::Config::new_from_args() {
        config::Config::Prefiltering(config) => {
            config.check()?;
            log::info!("Start pre-filtering!");
            prefiltering::PrefilteringRunner::run(config).await?;
        }
        config::Config::Filtering(config) => {
            config.check()?;
            log::info!("Start filtering!");
            filtering::FilteringRunner::run(config).await?;
        }
        config::Config::Condensation(config) => {
            config.check()?;
            log::info!("Start condensation!");
            condensing::CondensingRunner::run(config).await?;
        }
        config::Config::Transcription(config) => {
            config.check()?;
            log::info!("Start transcribing!");
            transcribing::Transcriptor::transcribe(&config)?;
        }
        config::Config::Analysis(config) => {
            config.check()?;
            log::info!("Start analysis!");
            analysis::AnalysisRunner::run(config).await?;
        }
        config::Config::Connection(config) => {
            config.check()?;
            log::info!("Start connecting!");
            connecting::ConnectionRunner::run(config).await?;
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(err) =
        fern::Dispatch::new().level(log::LevelFilter::Info).chain(std::io::stdout()).apply()
    {
        println!("Logger error:\n{err}");
        return;
    }

    let start_time = std::time::Instant::now();

    if let Err(err) = run().await {
        log::error!("Processing error:\n{err}");
    }

    log::info!("Done! Elapsed time: {}", utils::format_elapsed_time(start_time.elapsed()));
}
