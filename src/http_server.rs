use crate::App;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

const CHECK_TIMEOUT: Duration = Duration::from_secs(5);

pub async fn run(app: App, shutdown: CancellationToken, port: u16) -> anyhow::Result<()> {
    let router = Router::new()
        .route("/liveness", get(liveness))
        .route("/readiness", get(readiness))
        .route("/health", get(readiness))
        .with_state(app);

    let addr = format!("0.0.0.0:{port}");
    let listener = TcpListener::bind(&addr).await?;
    tracing::info!(addr = %addr, "http server listening");

    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            shutdown.cancelled().await;
            tracing::info!("http server shutting down");
        })
        .await?;

    Ok(())
}

async fn liveness() -> Json<Value> {
    Json(json!({ "status": "UP" }))
}

async fn readiness(State(app): State<App>) -> (StatusCode, Json<Value>) {
    let checks =
        async { tokio::join!(app.store.ping(), app.kafka.ping(&app.cfg.kafka.data_topic)) };

    let Ok((redis, kafka)) = tokio::time::timeout(CHECK_TIMEOUT, checks).await else {
        tracing::warn!("readiness check timed out");
        return readiness_response(false, false);
    };

    if let Err(e) = &redis {
        tracing::warn!(error = %e, "readiness: redis down");
    }
    if let Err(e) = &kafka {
        tracing::warn!(error = %e, "readiness: kafka down");
    }

    readiness_response(redis.is_ok(), kafka.is_ok())
}

fn readiness_response(redis_up: bool, kafka_up: bool) -> (StatusCode, Json<Value>) {
    let up = redis_up && kafka_up;
    let code = if up {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    let body = json!({
        "status": status_of(up),
        "components": {
            "redis": { "status": status_of(redis_up) },
            "kafka": { "status": status_of(kafka_up) },
        }
    });

    (code, Json(body))
}

fn status_of(up: bool) -> &'static str {
    if up { "UP" } else { "DOWN" }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_is_up_only_when_every_dependency_is_up() {
        let (code, Json(body)) = readiness_response(true, true);

        assert_eq!(code, StatusCode::OK);
        assert_eq!(body["status"], "UP");
        assert_eq!(body["components"]["redis"]["status"], "UP");
        assert_eq!(body["components"]["kafka"]["status"], "UP");
    }

    #[test]
    fn one_dependency_down_fails_the_whole_probe() {
        for (redis_up, kafka_up) in [(false, true), (true, false), (false, false)] {
            let (code, Json(body)) = readiness_response(redis_up, kafka_up);

            assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(body["status"], "DOWN");
            assert_eq!(body["components"]["redis"]["status"], status_of(redis_up));
            assert_eq!(body["components"]["kafka"]["status"], status_of(kafka_up));
        }
    }

    #[tokio::test]
    async fn liveness_never_reports_on_a_dependency() {
        let Json(body) = liveness().await;

        assert_eq!(body["status"], "UP");
        assert!(body.get("components").is_none());
    }
}
