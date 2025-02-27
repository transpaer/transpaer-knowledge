// TODO:
// #[deny(clippy::unwrap_used)]
// #[deny(clippy::expect_used)]

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use clap::Parser;

mod context;
mod errors;
mod models;
mod retrieve;
mod server;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    db_path: String,
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
            ))
        })
        .level(log::LevelFilter::Info)
        .chain(std::io::stdout())
        .apply()
    {
        println!("Logger error:\n{err}");
        return;
    }

    log::info!("Starting Sustainity backend!");

    let args = Args::parse();
    let retriever = retrieve::Retriever::new(&args.db_path).expect("DB error");

    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 8080));
    let server = server::Server::new(retriever);
    let service = sustainity_api::server::MakeService::new(server);
    let service = context::MakeAddContext::<_, context::EmptyContext>::new(service);
    hyper::server::Server::bind(&addr).serve(service).await.expect("Service failed")
}
