use clap::Parser;
use sqlrest::{http::Server, registry::Registry};
use std::net::SocketAddr;

#[derive(Parser)]
#[command(
    version,
    about = "Typed SQL HTTP interfaces. No built-in authentication; protect both listeners."
)]
struct Arguments {
    #[arg(long)]
    data_listen: SocketAddr,
    #[arg(long)]
    management_listen: SocketAddr,
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let arguments = Arguments::parse();
    match run(arguments).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run(arguments: Arguments) -> Result<(), sqlrest::SqlrestError> {
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|_| {
        sqlrest::SqlrestError::new(
            500,
            "signal_failed",
            "Cannot install shutdown signal handler",
        )
    })?;
    let server = Server::bind(
        Registry::new(),
        arguments.data_listen,
        arguments.management_listen,
    )
    .await?;
    eprintln!(
        "data={} management={}",
        server.data_address(),
        server.management_address()
    );
    server
        .serve(async move {
            #[cfg(unix)]
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = terminate.recv() => {},
            }
            #[cfg(not(unix))]
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
}
