use crate::config::KafkaConfig;
use anyhow::Context;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio_util::task::TaskTracker;

const PING_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone)]
pub struct KafkaSink {
    producer: FutureProducer,
    inflight: Arc<Semaphore>,
    deliveries: TaskTracker,
}

pub(crate) fn client_config(cfg: &KafkaConfig) -> ClientConfig {
    let mut client_config = ClientConfig::new();
    client_config.set("bootstrap.servers", &cfg.brokers);

    if let Some(protocol) = &cfg.security_protocol {
        client_config.set("security.protocol", protocol);
    }
    if let Some(mechanism) = &cfg.sasl_mechanism {
        client_config.set("sasl.mechanisms", mechanism);
    }
    if let Some(username) = &cfg.sasl_username {
        client_config.set("sasl.username", username);
    }
    if let Some(password) = &cfg.sasl_password {
        client_config.set("sasl.password", password);
    }

    client_config
}

impl KafkaSink {
    pub fn new(cfg: &KafkaConfig) -> anyhow::Result<Self> {
        let producer: FutureProducer = client_config(cfg)
            .create()
            .context("creating kafka producer")?;

        Ok(Self {
            producer,
            inflight: Arc::new(Semaphore::new(cfg.max_in_flight)),
            deliveries: TaskTracker::new(),
        })
    }

    pub async fn publish<T: Serialize>(&self, topic: &str, key: &str, message: T) {
        let payload = match serde_json::to_vec(&message) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(topic, error = %e, "failed to encode kafka payload");
                return;
            }
        };

        let Ok(permit) = self.inflight.clone().acquire_owned().await else {
            return;
        };

        let record = FutureRecord::to(topic).key(key).payload(&payload);
        let delivery = match self.producer.send_result(record) {
            Ok(delivery) => delivery,
            Err((e, _)) => {
                tracing::error!(topic, error = %e, "kafka enqueue failed");
                return;
            }
        };

        let topic = topic.to_string();
        self.deliveries.spawn(async move {
            let _permit = permit;
            match delivery.await {
                Ok(Ok(_)) => {}
                Ok(Err((e, _))) => tracing::error!(topic, error = %e, "kafka publish failed"),
                Err(_) => tracing::error!(topic, "kafka delivery report dropped"),
            }
        });
    }

    pub async fn wait_for_inflight(&self) {
        self.deliveries.close();
        let pending = self.deliveries.len();
        if pending > 0 {
            tracing::info!(pending, "waiting for in-flight kafka messages");
        }
        self.deliveries.wait().await;
    }

    pub async fn ping(&self, topic: &str) -> anyhow::Result<()> {
        let producer = self.producer.clone();
        let topic = topic.to_string();

        tokio::task::spawn_blocking(move || {
            producer
                .client()
                .fetch_metadata(Some(&topic), PING_TIMEOUT)
                .map(|_| ())
        })
        .await
        .context("kafka metadata probe panicked")?
        .context("fetching kafka metadata")
    }
}
