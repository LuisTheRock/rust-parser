use crate::processing::Topic;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("invalid value for {0}: {1}")]
    InvalidValue(String, String),
}

#[derive(Debug, Clone)]
pub struct Config {
    pub mqtt: MqttConfig,
    pub kafka: KafkaConfig,
    pub redis: RedisConfig,
    pub grpc: GrpcConfig,
    pub dictionary: DictionaryConfig,
    pub http_port: u16,
    pub ts_limits: TsLimits,
}

/// How far from "now" an incoming `_ts` may sit before it is reported as
/// bogus. Split out of [`Config`] so the pure normalization core can take it
/// without depending on broker or store settings.
#[derive(Debug, Clone, Copy)]
pub struct TsLimits {
    pub max_after_now_secs: u64,
    pub max_before_now_secs: u64,
}

#[derive(Debug, Clone)]
pub struct MqttConfig {
    pub broker_host: String,
    pub broker_port: u16,
    pub client_id: String,
    pub topics: Vec<String>,
    pub keep_alive_secs: u64,
    pub ca_cert_pem: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RedisConfig {
    pub url: String,
    pub ca_cert_pem: Option<String>,
}

#[derive(Debug, Clone)]
pub struct GrpcConfig {
    pub host_directory_address: String,
}

#[derive(Debug, Clone)]
pub struct DictionaryConfig {
    pub base_url: String,
}

#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub brokers: String,
    pub data_topic: String,
    pub events_topic: String,
    pub max_in_flight: usize,
    pub host_events_topic: String,
    pub dictionary_events_topic: String,
    pub consumer_group_id: String,
    pub security_protocol: Option<String>,
    pub sasl_mechanism: Option<String>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        Ok(Self {
            mqtt: MqttConfig::from_env()?,
            kafka: KafkaConfig::from_env()?,
            redis: RedisConfig::from_env(),
            grpc: GrpcConfig::from_env(),
            dictionary: DictionaryConfig::from_env(),
            http_port: env_parse("HTTP_PORT", 3000)?,
            ts_limits: TsLimits {
                max_after_now_secs: env_parse("MAX_TS_AFTER_NOW_SECS", 300)?,
                max_before_now_secs: env_parse("MAX_TS_BEFORE_NOW_SECS", 86400)?,
            },
        })
    }
}

impl MqttConfig {
    fn from_env() -> Result<Self, ConfigError> {
        Ok(Self {
            broker_host: strip_scheme(&env_string("MQTT_HOST", "localhost")),
            broker_port: env_parse("MQTT_PORT", 1883)?,
            // Must be unique per pod — brokers reject/kick a duplicate client_id.
            client_id: format!(
                "{}-{}",
                env_string("MQTT_CLIENT_ID", "mqtt-kafka-bridge"),
                hostname()
            ),
            topics: env_string("MQTT_TOPIC", "sensors/#")
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect(),
            keep_alive_secs: env_parse("MQTT_KEEP_ALIVE_SECS", 30)?,
            ca_cert_pem: non_empty_env("MQTT_CA_CERT_PEM").map(|s| s.replace("\\n", "\n")),
            username: non_empty_env("MQTT_USERNAME"),
            password: non_empty_env("MQTT_PASSWORD"),
        })
    }

    pub fn ca_cert_bytes(&self) -> Option<Vec<u8>> {
        self.ca_cert_pem
            .as_ref()
            .map(|pem| pem.clone().into_bytes())
    }
}

impl RedisConfig {
    fn from_env() -> Self {
        Self {
            url: env_string("REDIS_URL", "redis://127.0.0.1:6379"),
            ca_cert_pem: non_empty_env("REDIS_CA_CERT_PEM").map(|s| s.replace("\\n", "\n")),
        }
    }
}

impl GrpcConfig {
    fn from_env() -> Self {
        Self {
            host_directory_address: env_string(
                "HOST_DIRECTORY_GRPC_ADDRESS",
                "http://localhost:50051",
            ),
        }
    }
}

impl DictionaryConfig {
    fn from_env() -> Self {
        Self {
            base_url: env_string("DICTIONARY_BASE_URL", "http://localhost:8080"),
        }
    }
}

impl KafkaConfig {
    fn from_env() -> Result<Self, ConfigError> {
        Ok(Self {
            brokers: env_string("KAFKA_BROKERS", "localhost:9092"),
            data_topic: env_string("KAFKA_DATA_TOPIC", "data"),
            events_topic: env_string("KAFKA_EVENTS_TOPIC", "events"),
            max_in_flight: env_parse("KAFKA_MAX_IN_FLIGHT", 256)?,
            host_events_topic: env_string("KAFKA_HOST_EVENTS_TOPIC", "host-events"),
            dictionary_events_topic: env_string(
                "KAFKA_DICTIONARY_EVENTS_TOPIC",
                "dictionary-events",
            ),
            consumer_group_id: format!(
                "{}-{}",
                env_string("KAFKA_CONSUMER_GROUP_ID", "mqtt-kafka-bridge"),
                hostname()
            ),
            security_protocol: non_empty_env("KAFKA_SECURITY_PROTOCOL"),
            sasl_mechanism: non_empty_env("KAFKA_SASL_MECHANISM"),
            sasl_username: non_empty_env("KAFKA_SASL_USERNAME"),
            sasl_password: non_empty_env("KAFKA_SASL_PASSWORD"),
        })
    }
}

impl KafkaConfig {
    /// Resolves the logical stream chosen by [`crate::processing`] to the
    /// configured topic name.
    pub fn topic_for(&self, topic: Topic) -> &str {
        match topic {
            Topic::Data => &self.data_topic,
            Topic::Events => &self.events_topic,
        }
    }
}

fn hostname() -> String {
    gethostname::gethostname().to_string_lossy().into_owned()
}

/// An empty value (e.g. `HOST_DIRECTORY_GRPC_ADDRESS=` left blank in `.env`)
/// is treated the same as unset — falls back to `default` rather than
/// silently resolving to `""`, which is never a meaningful value for any of
/// these. Same semantics as `non_empty_env`.
fn env_string(key: &str, default: &str) -> String {
    non_empty_env(key).unwrap_or_else(|| default.to_string())
}

fn strip_scheme(host: &str) -> String {
    host.split_once("://")
        .map_or(host, |(_, rest)| rest)
        .to_string()
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|s| !s.is_empty())
}

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> Result<T, ConfigError> {
    match std::env::var(key) {
        Ok(v) => v
            .parse()
            .map_err(|_| ConfigError::InvalidValue(key.to_string(), v)),
        Err(_) => Ok(default),
    }
}
