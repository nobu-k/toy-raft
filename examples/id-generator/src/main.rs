use std::{convert::Infallible, sync::Arc};

use clap::Parser;
use http_body_util::BodyExt;
use prometheus::Encoder;
use tracing::info;

pub struct IdGenerator {
    next_id: std::sync::atomic::AtomicU64,
    last_applied_index: std::sync::atomic::AtomicU64,
}

impl IdGenerator {
    pub fn new() -> Self {
        IdGenerator {
            next_id: std::sync::atomic::AtomicU64::new(1),
            last_applied_index: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

#[async_trait::async_trait]
impl toy_raft::StateMachine for IdGenerator {
    async fn apply(
        &self,
        entry: toy_raft::log::Entry,
    ) -> Result<Option<toy_raft::ApplyResponse>, toy_raft::StateMachineError> {
        self.last_applied_index
            .store(entry.index().get(), std::sync::atomic::Ordering::SeqCst);
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(Some(Arc::new(id)))
    }

    async fn last_applied_index(&self) -> Result<toy_raft::Index, toy_raft::StateMachineError> {
        Ok(toy_raft::Index::new(
            self.last_applied_index
                .load(std::sync::atomic::Ordering::SeqCst),
        ))
    }
}

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
        .state_machine(std::sync::Arc::new(IdGenerator::new()))
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

    let server = toy_raft::Server::new(config).await?;
    let actor = server.actor();
    tokio::spawn(async move {
        // TODO: a new leader will not generate an ID. check what's going on.

        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

            let res = tokio::select! {
                res = actor.append_entry(Arc::new(vec![]), true) => res,
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(10)) => {
                    info!("ID generation timed out");
                    continue;
                }
            };

            match res {
                Ok(r) => match r {
                    Ok(Some(id)) => {
                        let id = id.downcast_ref::<u64>().unwrap();
                        info!(generated_id = id, "New ID generated");
                    }
                    Ok(None) => {
                        info!("No new ID generated");
                    }
                    Err(toy_raft::AppendEntryError::NotLeader(_)) => {}
                    Err(e) => {
                        info!(error = e.to_string(), "failed to append entry");
                    }
                },
                Err(e) => {
                    info!(error = e.to_string(), "failed to append entry");
                }
            }
        }
    });
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
