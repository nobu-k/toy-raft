#[tokio::main]
async fn main() -> anyhow::Result<()> {
    raftctl::run().await
}
