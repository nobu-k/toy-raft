/// Common arguments of all subcommands.
#[derive(Debug, clap::Args)]
pub struct GlobalArgs {
    /// List of the address of the server to connect to (comma separated or one address per flag).
    #[arg(short, long, env = "TOY_RAFT_SERVER")]
    server: Vec<String>,
}

impl GlobalArgs {
    /// Returns an iterator over the server addresses.
    pub fn servers(&self) -> anyhow::Result<impl Iterator<Item = &str>> {
        if self.server.is_empty() {
            return Err(anyhow::anyhow!("no server address provided"));
        }

        Ok(self
            .server
            .iter()
            .flat_map(|s| s.split(','))
            .map(|s| s.trim()))
    }
}
