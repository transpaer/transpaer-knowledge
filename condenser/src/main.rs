#![deny(clippy::pedantic)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![allow(clippy::module_name_repetitions)]

mod advisors;
mod cache;
mod categories;
mod condensing;
mod config;
mod errors;
mod filtering;
mod future_pool;
mod knowledge;
mod prefiltering;
mod processing;
mod transcribing;
mod utils;
mod wikidata;

use processing::Processor;

async fn run() -> Result<(), errors::ProcessingError> {
    match config::Config::new_from_args() {
        config::Config::Prefiltering(config) => {
            config.check()?;
            log::info!("Start pre-filtering!");
            prefiltering::PrefilteringProcessor::process(config).await?;
        }
        config::Config::Filtering(config) => {
            config.check()?;
            log::info!("Start filtering!");
            filtering::FilteringProcessor::process(config).await?;
        }
        config::Config::Condensation(config) => {
            config.check()?;
            log::info!("Start condensation!");
            condensing::CondensingProcessor::process(config).await?;
        }
        config::Config::Transcription(config) => {
            config.check()?;
            log::info!("Start transcribing!");
            transcribing::Transcriptor::transcribe(&config)?;
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(err) =
        fern::Dispatch::new().level(log::LevelFilter::Info).chain(std::io::stdout()).apply()
    {
        println!("Logger error: {err}");
        return;
    }

    let start_time = std::time::Instant::now();

    if let Err(err) = run().await {
        log::error!("Processing error: {err}");
    }

    log::info!("Done! Elapsed time: {}", utils::format_elapsed_time(start_time.elapsed()));
}
