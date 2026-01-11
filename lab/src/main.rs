// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#![deny(clippy::pedantic)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![allow(clippy::module_name_repetitions)]

/// Formats duration to a human-readable format.
#[must_use]
fn format_elapsed_time(duration: std::time::Duration) -> String {
    let duration = duration.as_secs();
    let seconds = duration % 60;
    let minutes = (duration / 60) % 60;
    let hours = duration / 3600;
    format!("{hours}h {minutes}m {seconds}s")
}

async fn run() -> Result<(), transpaer_lab::ProcessingError> {
    use transpaer_lab::Config;
    match Config::new_from_args() {
        Config::Absorbing(config) => {
            config.check()?;
            log::info!("Start absorbing");
            transpaer_lab::Absorber::run(&config).await?;
        }
        Config::Extracting(config) => {
            config.check()?;
            log::info!("Start extracting");
            transpaer_lab::ExtractingRunner::run(&config)?;
        }
        Config::Filtering(config) => {
            config.check()?;
            log::info!("Start filtering");
            transpaer_lab::FilteringRunner::run(&config)?;
        }
        Config::Updating(config) => {
            config.check()?;
            log::info!("Start updating!");
            transpaer_lab::UpdateRunner::run(&config)?;
        }
        Config::Condensation(config) => {
            config.check()?;
            log::info!("Start condensation!");
            transpaer_lab::CondensingRunner::run(&config)?;
        }
        Config::Coagulation(config) => {
            config.check()?;
            log::info!("Start coagulation!");
            transpaer_lab::Coagulator::run(&config)?;
        }
        Config::Crystalization(config) => {
            config.check()?;
            log::info!("Start crystalization!");
            transpaer_lab::Crystalizer::run(&config)?;
        }
        Config::Oxidation(config) => {
            config.check()?;
            log::info!("Start oxidizing!");
            transpaer_lab::Oxidizer::run(&config)?;
        }
        Config::Connection(config) => {
            config.check()?;
            log::info!("Start connecting!");
            // TODO: Remove the `connect` command.
            transpaer_lab::ConnectionRunner::run(&config)?;
        }
        Config::Sample(config) => {
            config.check()?;
            log::info!("Start sampling!");
            transpaer_lab::SamplingRunner::run(&config).await?;
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
        std::process::exit(1);
    }

    log::info!("Done! Elapsed time: {}", format_elapsed_time(start_time.elapsed()));
}

#[cfg(test)]
mod tests {
    use super::format_elapsed_time;

    #[test]
    fn test_format_elapsed_time() {
        use std::time::Duration;

        assert_eq!(format_elapsed_time(Duration::new(0, 0)), "0h 0m 0s");
        assert_eq!(format_elapsed_time(Duration::new(12, 0)), "0h 0m 12s");
        assert_eq!(format_elapsed_time(Duration::new(120, 0)), "0h 2m 0s");
        assert_eq!(format_elapsed_time(Duration::new(134, 0)), "0h 2m 14s");
        assert_eq!(format_elapsed_time(Duration::new(3600, 0)), "1h 0m 0s");
        assert_eq!(format_elapsed_time(Duration::new(3720, 0)), "1h 2m 0s");
        assert_eq!(format_elapsed_time(Duration::new(3724, 0)), "1h 2m 4s");
    }
}
