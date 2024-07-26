use clap::Parser;

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

    let server = toy_raft::Server::new(toy_raft::Config {
        id: args.id,
        addr: args.addr,
    })?;
    Ok(server.run().await?)
}
