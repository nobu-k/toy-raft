use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;

use crate::config;
use crate::grpc;
use crate::raft;

pub struct Server {
    addr: SocketAddr,
    state: Arc<Mutex<raft::Raft>>,
    log: Arc<Mutex<grpc::LogEntry>>,
}

impl Server {
    pub fn new(config: config::Config) -> Result<Server, ServerError> {
        let addr = config.addr.parse()?;

        let state = Arc::new(Mutex::new(raft::Raft::new()));

        let log = Arc::new(Mutex::new(grpc::LogEntry {
            term: 0,
            data: vec![],
        }));
        Ok(Server { addr, state, log })
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
        let msg = request.get_ref();

        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return Err(tonic::Status::internal("Internal error")),
        };
        Ok(tonic::Response::new(grpc::RequestVoteResponse {
            term: state.current_term(),
            vote_granted: state.grant_vote(msg),
        }))
    }
}
