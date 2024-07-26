#[derive(Debug, clap::Args)]
pub struct Args {}

pub async fn run(global_args: &crate::args::GlobalArgs, args: &Args) -> anyhow::Result<()> {
    let server = global_args
        .servers()?
        .next()
        .ok_or(anyhow::anyhow!("no server"))?
        .to_owned();
    let mut cli = toy_raft::grpc::operations_client::OperationsClient::connect(server).await?;
    let res = cli
        .status(tonic::Request::new(toy_raft::grpc::StatusRequest {}))
        .await?;
    println!("{:?}", res);
    Ok(())
}
