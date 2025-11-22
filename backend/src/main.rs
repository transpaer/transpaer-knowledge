// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// TODO:
// #[deny(clippy::unwrap_used)]
// #[deny(clippy::expect_used)]

use std::net::SocketAddr;

use clap::Parser;
use hyper::service::Service;
use tokio::net::TcpListener;

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

    log::info!("Starting Transpaer backend!");

    let args = Args::parse();
    let retriever = retrieve::Retriever::new(&args.db_path).expect("DB error");

    let server = server::Server::new(retriever);
    let service = transpaer_api::server::MakeService::new(server);
    let service = swagger::auth::MakeAllowAllAuthenticator::new(service, "cosmo");
    let service =
        transpaer_api::server::context::MakeAddContext::<_, swagger::EmptyContext>::new(service);

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    let listener = TcpListener::bind(addr).await.expect("Bind TCP listener");
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let service = service.call(addr).await.expect("Failed to accept connection");
                let io = hyper_util::rt::TokioIo::new(stream);
                tokio::task::spawn(async move {
                    if let Err(err) = hyper::server::conn::http1::Builder::new()
                        .serve_connection(io, service)
                        .await
                    {
                        eprintln!("Error serving connection: {:?}", err);
                    }
                });
            }
            Err(err) => eprintln!("Error accepting connection: {:?}", err),
        };
    }
}
