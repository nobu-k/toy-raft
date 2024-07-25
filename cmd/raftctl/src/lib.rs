mod args;
mod cmd;
mod status;

use clap::Parser;

pub fn run() -> anyhow::Result<()> {
    let root = cmd::Args::try_parse()?;

    match &root.command {
        cmd::Commands::Status(args) => status::run(&root.global_args, args),
    }
}
