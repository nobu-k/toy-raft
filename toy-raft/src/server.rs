use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;

use crate::config;
use crate::grpc;

pub struct Server {
    addr: SocketAddr,
    state: Arc<Mutex<State>>,
}

struct State {
    current_term: u64,
    voted_for: Option<String>,
    log: Vec<grpc::LogEntry>, // TODO: save this to storage
}

impl Server {
    pub fn new(config: config::Config) -> Result<Server, ServerError> {
        let addr = config.addr.parse()?;

        let state = Arc::new(Mutex::new(State {
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
        }));
        Ok(Server { addr, state })
    }

    pub async fn run(self) -> Result<(), ServerError> {
        let addr = self.addr.clone();
        match tonic::transport::Server::builder()
            .add_service(grpc::raft_server::RaftServer::new(self))
            .serve(addr)
            .await
        {
            Ok(_) => Ok(()),
            Err(_) => return Err(ServerError::InternalError),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("internal error")]
    InternalError,

    #[error(transparent)]
    InvalidAddress(#[from] std::net::AddrParseError),
}

#[tonic::async_trait]
impl grpc::raft_server::Raft for Server {
    async fn append_entries(
        &self,
        request: tonic::Request<grpc::AppendEntriesRequest>,
    ) -> Result<tonic::Response<grpc::AppendEntriesResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented"))
    }

    async fn request_vote(
        &self,
        request: tonic::Request<grpc::RequestVoteRequest>,
    ) -> Result<tonic::Response<grpc::RequestVoteResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented"))
    }
}
