#![deny(clippy::pedantic)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![allow(clippy::module_name_repetitions)]

use sustainity_lab::{config, errors};

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
        config::Config::Filtering1(config) => {
            config.check()?;
            log::info!("Start filtering, phase 1");
            sustainity_lab::filtering1::FilteringRunner::run(&config)?;
        }
        config::Config::Filtering2(config) => {
            config.check()?;
            log::info!("Start filtering, phase 2");
            sustainity_lab::filtering2::FilteringRunner::run(&config)?;
        }
        config::Config::Filtering(config) => {
            config.check()?;
            log::info!("Start filtering, phase 1");
            sustainity_lab::filtering1::FilteringRunner::run(&config.filter1)?;
            log::info!("Continue filtering, phase 2");
            sustainity_lab::filtering2::FilteringRunner::run(&config.filter2)?;
        }
        config::Config::Updating(config) => {
            config.check()?;
            log::info!("Start updating!");
            sustainity_lab::updating::UpdateRunner::run(&config)?;
        }
        config::Config::Condensation(config) => {
            config.check()?;
            log::info!("Start condensation!");
            sustainity_lab::condensing::CondensingRunner::run(&config)?;
        }
        config::Config::Crystalization(config) => {
            config.check()?;
            log::info!("Start crystalization!");
            sustainity_lab::crystalizing::Crystalizer::run(&config)?;
        }
        config::Config::Oxidation(config) => {
            config.check()?;
            log::info!("Start oxidizing!");
            sustainity_lab::oxidation::Oxidizer::run(&config)?;
        }
        config::Config::Analysis(config) => {
            config.check()?;
            log::info!("Start analysis!");
            sustainity_lab::analysis::AnalysisRunner::run(&config)?;
        }
        config::Config::Connection(config) => {
            config.check()?;
            log::info!("Start connecting!");
            sustainity_lab::connecting::ConnectionRunner::run(&config)?;
        }
        config::Config::Sample(config) => {
            config.check()?;
            log::info!("Start sampling!");
            sustainity_lab::sampling::SamplingRunner::run(&config).await?;
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(err) = fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "[{} {: <5}] {}",
                humantime::format_rfc3339_seconds(std::time::SystemTime::now()),
                record.level(),
                message
            ));
        })
        .level(log::LevelFilter::Info)
        .chain(std::io::stdout())
        .apply()
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
        sustainity_lab::utils::format_elapsed_time(start_time.elapsed())
    );
}
