use clap::Parser;
use tracing::info;

#[derive(Debug, Parser)]
struct Args {
    /// List of the address of the peer including scheme to connect to (comma separated or one address per flag).
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
