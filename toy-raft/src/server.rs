use std::{
    net::{SocketAddr, ToSocketAddrs},
    sync::Arc,
};

use tower_http::trace::{DefaultOnFailure, DefaultOnResponse, TraceLayer};
use tracing::{error, info_span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::{config, grpc, NodeState};

pub struct Server {
    id: String,
    addr: SocketAddr,
    actor: Arc<crate::actor::Actor>,
}

impl Server {
    pub async fn new(config: config::Config) -> Result<Server, ServerError> {
        let addrs: Vec<_> = config
            .addr
            .to_socket_addrs()
            .map_err(|e| ServerError::InvalidAddress(Arc::new(e)))?
            .collect();
        if addrs.len() != 1 {
            return Err(ServerError::TooManyAddresses(addrs.len(), config.addr));
        }
        let addr = addrs[0];

        let actor = Arc::new(
            crate::Actor::new(config.clone())
                .await
                .map_err(|e| ServerError::ActorError(e))?,
        );

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

                let context = opentelemetry::global::get_text_map_propagator(|propagator| {
                    propagator.extract(&Extractor {
                        metadata: &request.headers(),
                    })
                });

                let span = info_span!("toy_raft_server_request", id, method, path);
                span.set_parent(context);
                span
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

    pub fn actor(&self) -> Arc<crate::actor::Actor> {
        self.actor.clone()
    }
}

struct Extractor<'a> {
    metadata: &'a http::HeaderMap<http::HeaderValue>,
}

impl<'a> opentelemetry::propagation::Extractor for Extractor<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.metadata.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.metadata.keys().map(|k| k.as_str()).collect()
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

    #[error("failed to create actor: {0}")]
    ActorError(#[source] crate::StateMachineError),
}

#[tonic::async_trait]
impl grpc::raft_server::Raft for Arc<Server> {
    async fn append_entries(
        &self,
        request: tonic::Request<grpc::AppendEntriesRequest>,
    ) -> Result<tonic::Response<grpc::AppendEntriesResponse>, tonic::Status> {
        crate::metrics::APPEND_ENTRIES.inc();
        let msg = request.into_inner();
        let func = async {
            let res = self
                .actor
                .append_entries(msg)
                .await
                .map_err(|e| tonic::Status::internal(format!("Internal error: {}", e)))?;
            Ok(tonic::Response::new(res?))
        };
        func.await
            .inspect_err(|_| crate::metrics::APPEND_ENTRIES_FAILURE.inc())
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
                NodeState::Follower => grpc::State::Follower as i32,
                NodeState::Candidate => grpc::State::Candidate as i32,
                NodeState::Leader => grpc::State::Leader as i32,
            },
            leader_id: match state.state {
                NodeState::Leader => Some(self.id.clone()),
                _ => state.voted_for,
            },
        }))
    }
}
