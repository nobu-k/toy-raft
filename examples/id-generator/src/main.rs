#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Ok(toy_raft::Server::new(toy_raft::Config {
        id: "test".to_owned(),
        addr: "127.0.0.1:8080".to_owned(),
    })?
    .run()
    .await?)
}
