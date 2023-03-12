#![deny(clippy::pedantic)]

mod advisors;
mod cache;
mod categories;
mod config;
mod data_collector;
mod future_pool;
mod knowledge;
mod processing;
mod sources;
mod targets;
mod utils;

#[tokio::main]
async fn main() {
    if let Err(err) =
        fern::Dispatch::new().level(log::LevelFilter::Info).chain(std::io::stdout()).apply()
    {
        println!("Logger error: {err}");
        return;
    }

    let start_time = std::time::Instant::now();

    let config = config::Config::new_from_args();
    if let Err(err) = config.check() {
        log::error!("Coonfig error: {err}");
        return;
    }

    log::info!("Start processing!");

    processing::process(config).await;

    log::info!("Done! Elapsed time: {:?}", start_time.elapsed());
}
