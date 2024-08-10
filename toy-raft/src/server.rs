use std::{
    net::{SocketAddr, ToSocketAddrs},
    sync::Arc,
};

use tower_http::trace::{DefaultOnFailure, DefaultOnResponse, TraceLayer};
use tracing::{error, info_span};

use crate::{config, grpc, raft};

pub struct Server {
    id: String,
    addr: SocketAddr,
    actor: Arc<raft::Actor>,
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

        let actor = Arc::new(raft::Actor::new(config.clone()));

        Ok(Server {
            id: config.id,
            addr,
            actor,
        })
    }

    pub async fn run(self) -> Result<(), ServerError> {
        let addr = self.addr.clone();
        let server = Arc::new(self);
        let id = server.id.clone();

        let operations_service = tower::ServiceBuilder::new()
            .layer(
                TraceLayer::new_for_grpc()
                    .on_response(DefaultOnResponse::new().level(tracing::Level::INFO))
                    .on_failure(DefaultOnFailure::new().level(tracing::Level::INFO)),
            )
            .service(grpc::operations_server::OperationsServer::new(
                server.clone(),
            ))
            .into_inner();

        let raft_service = tower::ServiceBuilder::new()
            .layer(
                TraceLayer::new_for_grpc()
                    // Set this to TRACE as it's too verbose.
                    .on_response(DefaultOnResponse::new().level(tracing::Level::TRACE))
                    .on_failure(DefaultOnFailure::new().level(tracing::Level::INFO)),
            )
            .service(grpc::raft_server::RaftServer::new(server.clone()))
            .into_inner();

        match tonic::transport::Server::builder()
            .trace_fn(move |request| {
                let path = request.uri().path().to_owned();
                let method = request.method().to_string();
                info_span!("toy_raft_server", id, method, path)
            })
            .add_service(raft_service)
            .add_service(operations_service)
            .serve(addr)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                error!(error = e.to_string(), "Failed to serve");
                Err(e.into())
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("internal error")]
    InternalError,

    #[error("tonic server failed: {0}")]
    TonicServeError(#[from] tonic::transport::Error),

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
        let msg = request.into_inner();
        let res = self
            .actor
            .append_entries(msg)
            .await
            .map_err(|e| tonic::Status::internal(format!("Internal error: {}", e)))?;
        Ok(tonic::Response::new(res?))
    }

    async fn request_vote(
        &self,
        request: tonic::Request<grpc::RequestVoteRequest>,
    ) -> Result<tonic::Response<grpc::RequestVoteResponse>, tonic::Status> {
        let msg = request.into_inner();
        let (state, granted) = self
            .actor
            .grant_vote(msg)
            .await
            .map_err(|e| tonic::Status::internal(format!("Internal error: {}", e)))?;
        Ok(tonic::Response::new(grpc::RequestVoteResponse {
            term: state.current_term.get(),
            vote_granted: granted,
        }))
    }
}

#[tonic::async_trait]
impl grpc::operations_server::Operations for Arc<Server> {
    async fn status(
        &self,
        _: tonic::Request<grpc::StatusRequest>,
    ) -> Result<tonic::Response<grpc::StatusResponse>, tonic::Status> {
        let state = self.actor.state().await.map_err(|e| {
            error!(error = e.to_string(), "Error getting state");
            let mut s = tonic::Status::internal(format!("Internal error"));
            s.set_source(Arc::new(e));
            s
        })?;
        Ok(tonic::Response::new(grpc::StatusResponse {
            term: state.current_term.get(),
            state: match state.state {
                raft::NodeState::Follower => grpc::State::Follower as i32,
                raft::NodeState::Candidate => grpc::State::Candidate as i32,
                raft::NodeState::Leader => grpc::State::Leader as i32,
            },
            leader_id: match state.state {
                raft::NodeState::Leader => Some(self.id.clone()),
                _ => state.voted_for,
            },
        }))
    }
}
