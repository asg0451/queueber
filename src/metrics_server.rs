use std::convert::Infallible;
use std::net::SocketAddr;

use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto::Builder;
use http_body_util::Full;
use tokio::net::TcpListener;

use crate::metrics::{Encoder, QueueberMetrics, TextEncoder};

/// HTTP server for serving Prometheus metrics
pub struct MetricsServer {
    addr: SocketAddr,
}

impl MetricsServer {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }

    /// Start the metrics server - this runs until the server is shut down
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!("Starting metrics server on {}", self.addr);
        
        let listener = TcpListener::bind(self.addr).await?;
        
        loop {
            let (stream, _) = listener.accept().await?;
            let io = TokioIo::new(stream);
            
            tokio::task::spawn(async move {
                if let Err(err) = Builder::new(hyper_util::rt::TokioExecutor::new())
                    .serve_connection(io, service_fn(handle_request))
                    .await
                {
                    tracing::error!("Error serving metrics connection: {:?}", err);
                }
            });
        }
    }
}

async fn handle_request(
    req: Request<hyper::body::Incoming>,
) -> Result<Response<Full<hyper::body::Bytes>>, Infallible> {
    let response = match (req.method(), req.uri().path()) {
        (&hyper::Method::GET, "/metrics") => {
            match gather_metrics() {
                Ok(metrics_output) => {
                    Response::builder()
                        .status(StatusCode::OK)
                        .header("Content-Type", "text/plain; version=0.0.4")
                        .body(Full::new(metrics_output.into()))
                        .unwrap()
                }
                Err(e) => {
                    tracing::error!("Failed to gather metrics: {}", e);
                    Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Full::new("Internal Server Error".into()))
                        .unwrap()
                }
            }
        }
        (&hyper::Method::GET, "/health") => {
            Response::builder()
                .status(StatusCode::OK)
                .body(Full::new("OK".into()))
                .unwrap()
        }
        _ => {
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new("Not Found".into()))
                .unwrap()
        }
    };

    Ok(response)
}

fn gather_metrics() -> Result<String, Box<dyn std::error::Error>> {
    let encoder = TextEncoder::new();
    let metric_families = QueueberMetrics::registry().gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer)?;
    Ok(String::from_utf8(buffer)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    
    #[tokio::test]
    async fn test_metrics_endpoint() {
        crate::metrics::init();
        
        // Test the gather_metrics function
        let metrics = gather_metrics().expect("Should gather metrics");
        assert!(metrics.contains("queueber_"));
        assert!(metrics.contains("# HELP"));
        assert!(metrics.contains("# TYPE"));
    }
}