#![deny(clippy::pedantic)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![allow(clippy::module_name_repetitions)]

use sustainity_condensing::{config, errors, processing::Runnable};

/// Formats duration to a human-readable format.
#[must_use]
pub fn format_elapsed_time(duration: std::time::Duration) -> String {
    let duration = duration.as_secs();
    let seconds = duration % 60;
    let minutes = (duration / 60) % 60;
    let hours = duration / 3600;
    format!("{hours}h {minutes}m {seconds}s")
}

async fn run() -> Result<(), errors::ProcessingError> {
    match config::Config::new_from_args() {
        config::Config::Prefiltering(config) => {
            config.check()?;
            log::info!("Start pre-filtering!");
            sustainity_condensing::prefiltering::PrefilteringRunner::run(config).await?;
        }
        config::Config::Filtering(config) => {
            config.check()?;
            log::info!("Start filtering!");
            sustainity_condensing::filtering::FilteringRunner::run(config).await?;
        }
        config::Config::Updating(config) => {
            config.check()?;
            log::info!("Start updating!");
            sustainity_condensing::updating::UpdateRunner::run(config).await?;
        }
        config::Config::Condensation(config) => {
            config.check()?;
            log::info!("Start condensation!");
            sustainity_condensing::condensing::CondensingRunner::run(config).await?;
        }
        config::Config::Transcription(config) => {
            config.check()?;
            log::info!("Start transcribing!");
            sustainity_condensing::transcribing::Transcriptor::transcribe(&config)?;
        }
        config::Config::Analysis(config) => {
            config.check()?;
            log::info!("Start analysis!");
            sustainity_condensing::analysis::AnalysisRunner::run(config).await?;
        }
        config::Config::Connection(config) => {
            config.check()?;
            log::info!("Start connecting!");
            sustainity_condensing::connecting::ConnectionRunner::run(config).await?;
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

    log::info!(
        "Done! Elapsed time: {}",
        sustainity_condensing::utils::format_elapsed_time(start_time.elapsed())
    );
}
