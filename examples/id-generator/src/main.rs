#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    toy_raft::Server::new(toy_raft::Config {
        id: "test".to_owned(),
        addr: "localhost:8080".to_owned(),
    })?
    .run()
    .await?;
    Ok(())
}
