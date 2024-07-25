#[derive(Debug, clap::Args)]
pub struct Args {}

pub fn run(global_args: &crate::args::GlobalArgs, args: &Args) -> anyhow::Result<()> {
    for s in global_args.servers()? {
        println!("server: {}", s);
    }
    Ok(())
}
