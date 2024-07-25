#[derive(Debug, clap::Parser)]
pub struct Args {
    #[command(flatten)]
    pub global_args: crate::args::GlobalArgs,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, clap::Subcommand)]
pub enum Commands {
    Status(crate::status::Args),
}
