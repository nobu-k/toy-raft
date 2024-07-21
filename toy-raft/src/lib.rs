mod config;
mod grpc;

pub use config::Config;

use std::io::Error;

pub struct Server {}

impl Server {
    pub fn new(config: Config) -> Result<Server, Error> {
        Ok(Server {})
    }

    pub async fn run(&self) -> Result<(), Error> {
        Ok(())
    }
}
