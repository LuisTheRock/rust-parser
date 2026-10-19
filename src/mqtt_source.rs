use crate::App;
use crate::convert::map_to_string_map;
use rumqttc::{
    AsyncClient, Event, MqttOptions, Packet, QoS, SubscribeFilter, TlsConfiguration, Transport,
};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub async fn run(app: App, shutdown: CancellationToken) -> anyhow::Result<()> {
    let cfg = &app.cfg;
    let mut options = MqttOptions::new(
        &cfg.mqtt.client_id,
        &cfg.mqtt.broker_host,
        cfg.mqtt.broker_port,
    );
    options.set_keep_alive(Duration::from_secs(cfg.mqtt.keep_alive_secs));

    if let Some(ca) = cfg.mqtt.ca_cert_bytes() {
        options.set_transport(Transport::tls_with_config(TlsConfiguration::SimpleNative {
            ca,
            client_auth: None,
        }));
        tracing::info!("mqtt connection using TLS with provided CA cert");
    }

    if let Some(username) = &cfg.mqtt.username {
        options.set_credentials(username, cfg.mqtt.password.clone().unwrap_or_default());
        tracing::info!(username = %username, "mqtt connection using credentials");
    }

    let (client, mut eventloop) = AsyncClient::new(options, 100);

    // Subscriptions are (re)sent on every ConnAck, never once up front:
    // rumqttc defaults to clean_session, so the broker forgets them across a
    // reconnect and replays only unacked publishes. Subscribing once would
    // leave the service connected but deaf after the first broker blip.
    let filters: Vec<SubscribeFilter> = cfg
        .mqtt
        .topics
        .iter()
        .map(|topic| SubscribeFilter::new(topic.clone(), QoS::AtMostOnce))
        .collect();

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("mqtt shutdown requested, disconnecting");
                match client.disconnect().await {
                    Ok(()) => tracing::info!("mqtt disconnected cleanly"),
                    Err(e) => tracing::error!(error = %e, "mqtt disconnect failed"),
                }
                break;
            }
            event = eventloop.poll() => {
                match event {
                     Ok(Event::Incoming(Packet::ConnAck(_))) => {
                        // try_ variant: the eventloop is not being polled
                        // while this arm runs, so an awaiting send could
                        // deadlock against a full request channel.
                        client.try_subscribe_many(filters.clone())?;
                        tracing::info!(
                            host = %cfg.mqtt.broker_host,
                            port = cfg.mqtt.broker_port,
                            topics = ?cfg.mqtt.topics,
                            "mqtt connection established, subscriptions sent"
                        );
                    }
                    Ok(Event::Incoming(Packet::Publish(publish))) => {
                        match serde_json::from_slice::<serde_json::Map<String, serde_json::Value>>(&publish.payload) {
                            Ok(data) => {
                                let mut data = map_to_string_map(&data);
                                let ts_millis = extract_ts(&publish.topic, data.remove("_ts"));
                                let imei = extract_iot_id(&publish.topic);
                                data.remove("_type");
                                tracing::info!(
                                    topic = %publish.topic,
                                    imei = ?imei,
                                    timestamp = ts_millis,
                                    ?data,
                                    "received mqtt message"
                                );
                                app.ingest(&publish.topic, data, ts_millis, imei).await;
                            }
                            Err(e) => {
                                tracing::warn!(
                                    topic = %publish.topic,
                                    payload_len = publish.payload.len(),
                                    error = %e,
                                    "received mqtt message with non-json payload"
                                );
                            }
                        }
                    }
                    Ok(_) => {} // other acks, pings — nothing to do yet
                    Err(e) => {
                        tracing::error!(error = %e, "mqtt connection error, retrying");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }
    }

    Ok(())
}

fn extract_iot_id(topic: &str) -> Option<&str> {
    match topic.split('/').nth(1) {
        Some(imei) if !imei.is_empty() => Some(imei),
        _ => {
            tracing::warn!(topic = %topic, "could not extract imei from topic");
            None
        }
    }
}

fn extract_ts(topic: &str, raw: Option<String>) -> Option<i64> {
    let Some(raw) = raw else {
        tracing::warn!(topic = %topic, "message missing _ts key");
        return None;
    };
    match raw.parse::<i64>() {
        Ok(ts_millis) => Some(ts_millis),
        Err(_) => {
            tracing::warn!(topic = %topic, value = %raw, "_ts is not a valid integer (milliseconds)");
            None
        }
    }
}
