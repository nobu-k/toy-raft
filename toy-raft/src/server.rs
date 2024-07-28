use std::error::Error;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::sync::Mutex;

use hyper::body::Body;
use tracing::info_span;
use tracing::{error, info};

use crate::config;
use crate::grpc;
use crate::raft;

pub struct Server {
    id: String,
    addr: SocketAddr,
    state: Arc<Mutex<raft::Raft>>,
    log: Arc<Mutex<grpc::LogEntry>>,
}

#[derive(Debug, Clone)]
struct AccessLogging<S> {
    inner: S,
}

// Alternative: https://github.com/hyperium/tonic/blob/2d9791198fdec1b2e8530bd3365d0c0ddb290f4c/examples/src/tower/server.rs

type TonicRequest = hyper::Request<tonic::body::BoxBody>;
type TonicResponse = hyper::Response<tonic::body::BoxBody>;

impl<S> tower::Service<TonicRequest> for AccessLogging<S>
where
    S: tower::Service<TonicRequest, Response = TonicResponse, Error = tower::BoxError>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = AccessLoggingFuture<S::Future>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, request: TonicRequest) -> Self::Future {
        let size = request.size_hint().lower();

        let f = self.inner.call(request);
        AccessLoggingFuture {
            inner_future: f,
            start: std::time::Instant::now(),
            size_hint: size,
        }
    }
}

pin_project_lite::pin_project! {
    struct AccessLoggingFuture<F> {
        #[pin]
        inner_future: F,
        start: std::time::Instant,
        size_hint: u64,
    }
}

impl<F> std::future::Future for AccessLoggingFuture<F>
where
    F: std::future::Future<Output = Result<TonicResponse, tower::BoxError>>,
{
    type Output = F::Output;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let project = self.project();
        let elapsed = project.start.elapsed().as_secs_f64();
        match project.inner_future.poll(cx) {
            std::task::Poll::Ready(result) => {
                match &result {
                    Ok(res) => info!(
                        elapsed,
                        grpcStatus = tonic::Code::Ok as i32,
                        size = project.size_hint,
                        httpStatus = res.status().as_u16(),
                        "Access"
                    ),
                    Err(e) => {
                        let status = if let Some(status) = e.downcast_ref::<tonic::Status>() {
                            status.code()
                        } else {
                            tonic::Code::Unknown
                        };
                        info!(
                            err = e.to_string(),
                            grpcStatus = status as i32,
                            elapsed,
                            size = project.size_hint,
                            "Access"
                        );
                    }
                }

                std::task::Poll::Ready(result)
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

#[derive(Debug, Clone)]
struct AccessLoggingLayer;

impl<S> tower::Layer<S> for AccessLoggingLayer {
    type Service = AccessLogging<S>;

    fn layer(&self, service: S) -> Self::Service {
        AccessLogging { inner: service }
    }
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
            .layer(AccessLoggingLayer)
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
