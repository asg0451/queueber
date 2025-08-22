use axum::{Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};
use prometheus_client::registry::Registry;
use std::sync::Arc;

use tower_http::cors::CorsLayer;

use crate::metrics_atomic::{AtomicMetrics, encode_atomic_metrics};

pub struct MetricsServer {
    #[allow(dead_code)]
    registry: Registry,
    #[allow(dead_code)]
    metrics: AtomicMetrics,
}

impl MetricsServer {
    pub fn new(registry: Registry, metrics: AtomicMetrics) -> Self {
        Self { registry, metrics }
    }

    pub async fn start(self, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        let app = Router::new()
            .route("/metrics", get(metrics_handler))
            .route("/health", get(health_handler))
            .layer(CorsLayer::permissive())
            .with_state(Arc::new(self));

        let listener = tokio::net::TcpListener::bind(addr).await?;
        tracing::info!("Metrics server listening on {}", addr);

        axum::serve(listener, app).await?;
        Ok(())
    }
}

async fn metrics_handler(State(state): State<Arc<MetricsServer>>) -> impl IntoResponse {
    let snapshot = state.metrics.snapshot();
    match encode_atomic_metrics(&snapshot) {
        Ok(metrics) => (StatusCode::OK, metrics).into_response(),
        Err(e) => {
            tracing::error!("Failed to encode metrics: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to encode metrics",
            )
                .into_response()
        }
    }
}

async fn health_handler() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}
