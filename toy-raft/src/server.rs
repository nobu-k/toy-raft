use std::{
    net::{SocketAddr, ToSocketAddrs},
    sync::{Arc, Mutex},
};

use tracing::info_span;

use crate::{config, grpc, raft};

pub struct Server {
    id: String,
    addr: SocketAddr,
    state: Arc<Mutex<raft::Raft>>,
    log: Arc<Mutex<grpc::LogEntry>>,
}
impl Server {
    pub fn new(config: config::Config) -> Result<Server, ServerError> {
        let addrs: Vec<_> = config
            .addr
            .to_socket_addrs()
            .map_err(|e| ServerError::InvalidAddress(Arc::new(e)))?
            .collect();
        if addrs.len() != 1 {
            return Err(ServerError::TooManyAddresses(addrs.len(), config.addr));
        }
        let addr = addrs[0];

        let state = Arc::new(Mutex::new(raft::Raft::new()));

        let log = Arc::new(Mutex::new(grpc::LogEntry {
            term: 0,
            data: vec![],
        }));
        Ok(Server {
            id: config.id,
            addr,
            state,
            log,
        })
    }

    pub async fn run(self) -> Result<(), ServerError> {
        let addr = self.addr.clone();
        let server = Arc::new(self);
        let id = server.id.clone();

        match tonic::transport::Server::builder()
            .trace_fn(move |request| {
                let path = request.uri().path().to_owned();
                let method = request.method().to_string();
                info_span!("toy_raft_server", id, method, path)
            })
            .layer(crate::access_log::AccessLoggingLayer)
            .add_service(grpc::raft_server::RaftServer::new(server.clone()))
            .add_service(grpc::operations_server::OperationsServer::new(
                server.clone(),
            ))
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

    #[error("invalid address: {}", .0)]
    InvalidAddress(#[source] Arc<dyn std::error::Error + Sync + Send + 'static>),

    #[error("too many addresses: {}: {}", .0, .1)]
    TooManyAddresses(usize, String),
}

#[tonic::async_trait]
impl grpc::raft_server::Raft for Arc<Server> {
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

#[tonic::async_trait]
impl grpc::operations_server::Operations for Arc<Server> {
    async fn status(
        &self,
        _: tonic::Request<grpc::StatusRequest>,
    ) -> Result<tonic::Response<grpc::StatusResponse>, tonic::Status> {
        let state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return Err(tonic::Status::internal("Internal error")),
        };

        Ok(tonic::Response::new(grpc::StatusResponse {
            term: state.current_term(),
            state: match state.state() {
                raft::State::Follower => grpc::State::Follower as i32,
                raft::State::Candidate => grpc::State::Candidate as i32,
                raft::State::Leader => grpc::State::Leader as i32,
            },
            leader_id: match state.state() {
                raft::State::Leader => Some(self.id.clone()),
                _ => state.voted_for(),
            },
        }))
    }
}
