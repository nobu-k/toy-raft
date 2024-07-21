use anyhow::Result;

fn main() -> Result<()> {
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile(&["../proto/raft.proto"], &["../proto"])?;
    Ok(())
}
