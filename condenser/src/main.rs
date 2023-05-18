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
mod filtering_manufacturers;
mod filtering_products;
mod future_pool;
mod knowledge;
mod processing;
mod transcribing;
mod utils;
mod wikidata;

use processing::Processor;

async fn run() -> Result<(), errors::ProcessingError> {
    match config::Config::new_from_args() {
        config::Config::FilterProducts(config) => {
            config.check()?;
            log::info!("Start filtering products!");
            filtering_products::ProductProcessor::process(config).await?;
        }
        config::Config::FilterManufacturers(config) => {
            config.check()?;
            log::info!("Start filtering manufacturers!");
            filtering_manufacturers::ManufacturerProcessor::process(config).await?;
        }
        config::Config::Condense(config) => {
            config.check()?;
            log::info!("Start condensing!");
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
