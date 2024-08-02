use std::convert::Infallible;

use clap::Parser;
use http_body_util::BodyExt;
use prometheus::Encoder;
use tracing::info;

#[derive(Debug, Parser)]
struct Args {
    /// List of the address of the peer including scheme to connect to (comma separated or one address per flag).
    #[arg(long)]
    peer: Vec<String>,

    /// ID of this server.
    #[arg(long, env = "TOY_RAFT_SERVER_ID")]
    id: String,

    /// Address to bind to.
    #[arg(long, env = "TOY_RAFT_SERVER_ADDR")]
    addr: String,

    /// Metrics address to bind to.
    #[arg(long)]
    metrics_addr: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::try_parse()?;

    let config = toy_raft::Config::builder()
        .id(args.id.clone())
        .addr(args.addr.clone())
        .peers(toy_raft::Peer::parse_args(&args.peer)?)
        .build()?;

    let subscriber = tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        .with_current_span(false)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_file(true)
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    info!(addr = args.addr, id = args.id, "Starting server");

    if let Some(addr) = args.metrics_addr {
        tokio::spawn(async move {
            // TODO: shutdown the server if the metrics server couldn't start.
            if let Err(e) = run_metrics_server(addr).await {
                info!(
                    error = anyhow::anyhow!(e).to_string(),
                    "failed to start metrics server"
                );
            }
        });
    }

    let server = toy_raft::Server::new(config)?;
    Ok(server.run().await?)
}

async fn run_metrics_server(addr: String) -> anyhow::Result<()> {
    let addr = tokio::net::lookup_host(addr).await?.next().unwrap();
    let listener = tokio::net::TcpListener::bind(addr).await?;

    loop {
        let (socket, _) = match listener.accept().await {
            Ok(res) => res,
            Err(e) => {
                info!(
                    error = anyhow::anyhow!(e).to_string(),
                    "failed to accept connection"
                );
                continue;
            }
        };

        let io = hyper_util::rt::TokioIo::new(socket);

        tokio::spawn(async move {
            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, hyper::service::service_fn(handle_metrics_connection))
                .await
            {
                info!(
                    error = anyhow::anyhow!(e).to_string(),
                    "failed to serve connection"
                );
            }
        });
    }
}

async fn handle_metrics_connection(
    req: hyper::Request<hyper::body::Incoming>,
) -> Result<
    hyper::Response<http_body_util::combinators::BoxBody<hyper::body::Bytes, Infallible>>,
    std::convert::Infallible,
> {
    // TODO: error handling
    // TODO: add tokio metrics
    if req.method() == hyper::Method::GET && req.uri().path() == "/metrics" {
        let metric_families = prometheus::gather();
        let mut buffer = vec![];
        let encoder = prometheus::TextEncoder::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        info!(metrics = metric_families.len(), "encoded metrics");

        let body = http_body_util::Full::new(hyper::body::Bytes::from(buffer));
        Ok(hyper::Response::new(body.boxed()))
    } else {
        let response = hyper::Response::builder()
            .status(hyper::StatusCode::NOT_FOUND)
            .body(http_body_util::Empty::new().boxed())
            .unwrap();
        Ok(response)
    }
}
