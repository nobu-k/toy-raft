use crate::config;

pub struct Server {}

impl Server {
    pub fn new(config: config::Config) -> Result<Server, ServerError> {
        Ok(Server {})
    }

    pub async fn run(&self) -> Result<(), ServerError> {
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("internal error")]
    InternalError,
}
