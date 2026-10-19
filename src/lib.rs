pub mod big_data_store;
pub mod config;
pub mod convert;
pub mod dictionary;
pub mod host_directory;
pub mod http_server;
pub mod kafka_consumer;
pub mod kafka_sink;
pub mod message;
pub mod mqtt_source;
pub mod processing;
pub mod registry;

use crate::big_data_store::BigDataStore;
use crate::config::Config;
use crate::kafka_sink::KafkaSink;
use crate::registry::Registry;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct App {
    pub cfg: Arc<Config>,
    pub registry: Arc<Registry>,
    pub kafka: Arc<KafkaSink>,
    pub store: Arc<BigDataStore>,
}

impl App {
    #[must_use]
    pub fn new(
        cfg: Arc<Config>,
        registry: Arc<Registry>,
        kafka: Arc<KafkaSink>,
        store: Arc<BigDataStore>,
    ) -> Self {
        Self {
            cfg,
            registry,
            kafka,
            store,
        }
    }

    pub async fn ingest(
        &self,
        mqtt_topic: &str,
        fields: HashMap<String, String>,
        ts_millis: Option<i64>,
        imei: Option<&str>,
    ) {
        let Some(imei) = imei else {
            tracing::error!(topic = %mqtt_topic, "dropping message: no imei, cannot build normalized envelope");
            return;
        };

        let Some(host_id) = self.registry.imei_to_host(imei) else {
            tracing::error!(topic = %mqtt_topic, imei, "no hostId registered for imei, dropping message");
            return;
        };

        let last_ctx_id = if processing::needs_ctx_id(mqtt_topic, &fields) {
            self.last_ctx_id(&host_id).await
        } else {
            None
        };

        let Ok(routed) = processing::normalize(
            mqtt_topic,
            fields,
            ts_millis,
            imei,
            &host_id,
            last_ctx_id,
            &self.cfg.ts_limits,
        ) else {
            return; // normalize logged which rule dropped it
        };

        for message in routed {
            if !self.registry.is_valid_key(&message.body.payload.key) {
                tracing::error!(imei, key = %message.body.payload.key, "invalid data key, dropping field");
                continue;
            }
            let topic = self.cfg.kafka.topic_for(message.topic);
            self.kafka.publish(topic, imei, message.body).await;
        }
    }

    async fn last_ctx_id(&self, host_id: &str) -> Option<String> {
        match self.store.get_current_ctx_id(host_id).await {
            Ok(ctx_id) => {
                tracing::info!(host_id, ctx_id, "grabbed last ctx_id");
                Some(ctx_id)
            }
            Err(e) => {
                tracing::error!(host_id, error = %e, "failed to grab last ctx_id");
                None
            }
        }
    }
}

/// Spawns a long-lived component and cancels `shutdown` as soon as it stops,
/// however it stops.
///
/// Without this a failed task just logs and exits while `main` stays parked
/// on ctrl-c: the process keeps running, the probes keep passing, and the
/// pod quietly processes nothing.
pub fn supervise<F>(name: &'static str, shutdown: CancellationToken, task: F) -> JoinHandle<()>
where
    F: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    tokio::spawn(async move {
        match task.await {
            Ok(()) => tracing::info!(task = name, "task stopped"),
            Err(e) => tracing::error!(task = name, error = %e, "task failed"),
        }
        shutdown.cancel();
    })
}
