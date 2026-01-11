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

use tracing_subscriber::prelude::*;

mod errors;
mod models;
mod retrieve;
mod server;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    db_path: String,

    #[arg(short, long)]
    log_path: Option<String>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    setup_logger(args.log_path.as_ref());
    tracing::info!(
        build_date = env!("VERGEN_BUILD_TIMESTAMP"),
        commit = env!("VERGEN_GIT_SHA"),
        "Starting Transpaer backend!"
    );

    let retriever = retrieve::Retriever::new(&args.db_path).expect("DB error");

    let server = server::Server::new(retriever);
    let service = transpaer_api::server::MakeService::new(server);
    let service = swagger::auth::MakeAllowAllAuthenticator::new(service, "cosmo");
    let service =
        transpaer_api::server::context::MakeAddContext::<_, swagger::EmptyContext>::new(service);

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    let listener = TcpListener::bind(addr).await.expect("Bind TCP listener");
    tracing::info!("Listening on {:?}", addr);

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

fn setup_logger(log_path: Option<&String>) {
    let filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing::Level::INFO.into())
        .from_env_lossy();
    let output = tracing_subscriber::fmt::layer();

    if let Some(log_path) = log_path {
        let appender = tracing_appender::rolling::Builder::new()
            .rotation(tracing_appender::rolling::Rotation::MINUTELY)
            .filename_prefix("backend")
            .filename_suffix("log")
            .build(log_path)
            .expect("failed to initialize log file appender");
        let file =
            tracing_subscriber::fmt::layer().with_writer(appender).json().flatten_event(true);

        tracing_subscriber::registry().with(filter).with(output).with(file).init()
    } else {
        tracing_subscriber::registry().with(filter).with(output).init()
    }
}
