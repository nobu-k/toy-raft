use std::error::Error;

use hyper::body::Body;
use tracing::info;

#[derive(Debug, Clone)]
pub struct AccessLogging<S> {
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
    pub struct AccessLoggingFuture<F> {
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
                        // TODO: never comes here even if the RPC returns an error (i.e. Status).
                        // To leave the right access log, inspecting Ok(res) is necessary.
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
pub struct AccessLoggingLayer;

impl<S> tower::Layer<S> for AccessLoggingLayer {
    type Service = AccessLogging<S>;

    fn layer(&self, service: S) -> Self::Service {
        AccessLogging { inner: service }
    }
}
