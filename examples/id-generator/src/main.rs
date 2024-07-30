use clap::Parser;
use tracing::info;

#[derive(Debug, Parser)]
struct Args {
    /// List of the address of the peer including scheme to connect to (comma separated or one address per flag).
    // TODO: Considering the configuration in StatefulSet where all Pods have
    // the same args and envs, it is better to allow the peer to include itself.
    // The peer for self should be removed before passing to Config.
    #[arg(long)]
    peer: Vec<String>,

    /// ID of this server.
    id: String,

    /// Address to bind to.
    addr: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::try_parse()?;

    let config = toy_raft::Config::builder()
        .id(args.id.clone())
        .addr(args.addr.clone())
        // TODO: parse peers properly
        .peers(args.peer.clone())
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

    let server = toy_raft::Server::new(config)?;
    Ok(server.run().await?)
}
