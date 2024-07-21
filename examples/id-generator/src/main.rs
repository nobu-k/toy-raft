#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    toy_raft::Server::new(toy_raft::Config {
        id: "test".to_owned(),
        port: 8080,
    })?
    .run()
    .await?;
    Ok(())
}
