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

async fn run() -> Result<(), sustainity_lab::ProcessingError> {
    use sustainity_lab::Config;
    match Config::new_from_args() {
        Config::Extracting(config) => {
            config.check()?;
            log::info!("Start extracting");
            sustainity_lab::ExtractingRunner::run(&config)?;
        }
        Config::Filtering(config) => {
            config.check()?;
            log::info!("Start filtering");
            sustainity_lab::FilteringRunner::run(&config)?;
        }
        Config::Updating(config) => {
            config.check()?;
            log::info!("Start updating!");
            sustainity_lab::UpdateRunner::run(&config)?;
        }
        Config::Condensation(config) => {
            config.check()?;
            log::info!("Start condensation!");
            sustainity_lab::CondensingRunner::run(&config)?;
        }
        Config::Coagulation(config) => {
            config.check()?;
            log::info!("Start coagulation!");
            sustainity_lab::Coagulator::run(&config)?;
        }
        Config::Crystalization(config) => {
            config.check()?;
            log::info!("Start crystalization!");
            sustainity_lab::Crystalizer::run(&config)?;
        }
        Config::Oxidation(config) => {
            config.check()?;
            log::info!("Start oxidizing!");
            sustainity_lab::Oxidizer::run(&config)?;
        }
        Config::Connection(config) => {
            config.check()?;
            log::info!("Start connecting!");
            sustainity_lab::ConnectionRunner::run(&config)?;
        }
        Config::Sample(config) => {
            config.check()?;
            log::info!("Start sampling!");
            sustainity_lab::SamplingRunner::run(&config).await?;
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
