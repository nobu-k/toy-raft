#[derive(Debug, clap::Args)]
pub struct Args {}

pub async fn run(global_args: &crate::args::GlobalArgs, args: &Args) -> anyhow::Result<()> {
    let mut handlers = vec![];
    for server in global_args.servers()? {
        handlers.push(tokio::spawn(call_status(server.to_owned())));
    }

    let mut errs: Vec<anyhow::Result<()>> = vec![];
    for handler in handlers {
        match handler.await {
            Ok(res) => match res {
                Ok(res) => println!("{:?}", res),
                Err(e) => errs.push(Err(e.into())),
            },
            Err(e) => errs.push(Err(e.into())),
        }
    }

    if errs.is_empty() {
        Ok(())
    } else {
        Err(anyhow::anyhow!("failed to access some servers: {:?}", errs))
    }
}

async fn call_status(
    addr: String,
) -> anyhow::Result<tonic::Response<toy_raft::grpc::StatusResponse>> {
    let mut cli = toy_raft::grpc::operations_client::OperationsClient::connect(addr).await?;
    Ok(cli
        .status(tonic::Request::new(toy_raft::grpc::StatusRequest {}))
        .await?)
}
