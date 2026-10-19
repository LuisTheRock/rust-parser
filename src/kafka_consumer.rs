use crate::config::KafkaConfig;
use crate::kafka_sink::client_config;
use crate::processing::BOARD_TYPE;
use crate::registry::Registry;
use rdkafka::Message;
use rdkafka::consumer::{Consumer, StreamConsumer};
use serde::Deserialize;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

const ACTION_ADD: &str = "add";
const ACTION_REMOVE: &str = "remove";

#[derive(Deserialize)]
struct DeviceActionMessage {
    action: String,
    #[serde(rename = "deviceId")]
    device_id: String,
    #[serde(rename = "deviceImei")]
    device_imei: String,
}

#[derive(Deserialize)]
struct DictionaryKeyActionMessage {
    action: String,
    #[serde(rename = "boardType")]
    board_type: String,
    #[serde(rename = "rawKey")]
    raw_key: String,
}

pub async fn run(
    cfg: &KafkaConfig,
    shutdown: CancellationToken,
    registry: Arc<Registry>,
) -> anyhow::Result<()> {
    let mut consumer_config = client_config(cfg);
    consumer_config
        .set("group.id", &cfg.consumer_group_id)
        .set("auto.offset.reset", "latest");
    let consumer: StreamConsumer = consumer_config.create()?;

    consumer.subscribe(&[
        cfg.host_events_topic.as_str(),
        cfg.dictionary_events_topic.as_str(),
    ])?;
    tracing::info!(
        host_topic = %cfg.host_events_topic,
        dictionary_topic = %cfg.dictionary_events_topic,
        group_id = %cfg.consumer_group_id,
        "kafka consumer subscribed"
    );

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("kafka consumer shutting down");
                break;
            }
            result = consumer.recv() => {
                match result {
                    Ok(message) => handle_message(&message, cfg, &registry),
                    Err(e) => tracing::error!(error = %e, "kafka consumer error"),
                }
            }
        }
    }

    Ok(())
}

fn handle_message(
    message: &rdkafka::message::BorrowedMessage,
    cfg: &KafkaConfig,
    registry: &Registry,
) {
    let topic = message.topic();
    let Some(payload) = message.payload() else {
        tracing::warn!(topic, "kafka message with empty payload");
        return;
    };

    if topic == cfg.host_events_topic {
        handle_device_action(payload, registry);
    } else if topic == cfg.dictionary_events_topic {
        handle_dictionary_key_action(payload, registry);
    } else {
        tracing::warn!(topic, "message from unexpected topic");
    }
}

fn handle_device_action(payload: &[u8], registry: &Registry) {
    let msg: DeviceActionMessage = match serde_json::from_slice(payload) {
        Ok(m) => m,
        Err(e) => {
            tracing::error!(error = %e, "failed to decode device action message");
            return;
        }
    };

    match msg.action.as_str() {
        ACTION_ADD => {
            tracing::info!(imei = %msg.device_imei, host_id = %msg.device_id, "adding host");
            registry.add_host(msg.device_imei, msg.device_id);
        }
        ACTION_REMOVE => {
            tracing::info!(imei = %msg.device_imei, "removing host");
            registry.remove_host(&msg.device_imei);
        }
        other => tracing::warn!(action = other, "unknown device action"),
    }
}

fn handle_dictionary_key_action(payload: &[u8], registry: &Registry) {
    let msg: DictionaryKeyActionMessage = match serde_json::from_slice(payload) {
        Ok(m) => m,
        Err(e) => {
            tracing::error!(error = %e, "failed to decode dictionary key action message");
            return;
        }
    };

    if msg.board_type != BOARD_TYPE {
        return;
    }

    match msg.action.as_str() {
        ACTION_ADD => {
            tracing::info!(key = %msg.raw_key, "adding valid key");
            registry.add_valid_key(msg.raw_key);
        }
        ACTION_REMOVE => {
            tracing::info!(key = %msg.raw_key, "removing valid key");
            registry.remove_valid_key(&msg.raw_key);
        }
        other => tracing::warn!(action = other, "unknown dictionary key action"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_action_add_then_remove() {
        let registry = Registry::new();

        handle_device_action(
            br#"{"action":"add","deviceId":"host-1","deviceImei":"123"}"#,
            &registry,
        );
        assert_eq!(registry.imei_to_host("123"), Some("host-1".to_string()));

        handle_device_action(
            br#"{"action":"remove","deviceImei":"123","deviceId":""}"#,
            &registry,
        );
        assert_eq!(registry.imei_to_host("123"), None);
    }

    #[test]
    fn device_action_unknown_is_ignored() {
        let registry = Registry::new();
        handle_device_action(
            br#"{"action":"rename","deviceId":"host-1","deviceImei":"123"}"#,
            &registry,
        );
        assert_eq!(registry.imei_to_host("123"), None);
    }

    #[test]
    fn dictionary_key_action_add_then_remove_for_matching_board_type() {
        let registry = Registry::new();

        handle_dictionary_key_action(
            format!(r#"{{"action":"add","boardType":"{BOARD_TYPE}","rawKey":"speed","normalizedKey":"Speed"}}"#)
                .as_bytes(),
            &registry,
        );
        assert!(registry.is_valid_key("speed"));

        handle_dictionary_key_action(
            format!(r#"{{"action":"remove","boardType":"{BOARD_TYPE}","rawKey":"speed","normalizedKey":"Speed"}}"#)
                .as_bytes(),
            &registry,
        );
        assert!(!registry.is_valid_key("speed"));
    }

    #[test]
    fn dictionary_key_action_ignores_other_board_types() {
        let registry = Registry::new();
        handle_dictionary_key_action(
            br#"{"action":"add","boardType":"OTHER","rawKey":"speed","normalizedKey":"Speed"}"#,
            &registry,
        );
        assert!(!registry.is_valid_key("speed"));
    }
}
