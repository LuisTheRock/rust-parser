use crate::config::TsLimits;
use crate::message::{NormalizedMessage, NormalizedMessagePayload};
use chrono::{DateTime, SecondsFormat, Utc};
use std::collections::HashMap;
use thiserror::Error;
use uuid::Uuid;

pub const BOARD_TYPE: &str = "CCPM";
const EVENT_ID_KEY: &str = "event_id";
const CURRENT_STATE_KEY: &str = "current_state";
const LATITUDE_KEY: &str = "latitude";
const LONGITUDE_KEY: &str = "longitude";
const POSITION_KEY: &str = "position";
const STATE_KEY: &str = "state";
const CTX_ID_KEY: &str = "ctx_id";
const STATE_REQ_UNLOCK: &str = "_REQ_UNLOCK_";
const STATE_LOCKED: &str = "_ST_LOCKED_";
const LAST_CTX_ID_KEY: &str = "last_ctx_id";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Topic {
    Data,
    Events,
}

#[derive(Debug)]
pub struct Routed {
    pub topic: Topic,
    pub body: NormalizedMessage,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DropReason {
    #[error("missing latitude/longitude")]
    MissingPosition,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TsError {
    #[error(
        "timestamp {ts_millis}ms is in the future (now {now_millis}ms, allowed up to +{allowed_millis}ms)"
    )]
    TooNew {
        ts_millis: i64,
        now_millis: i64,
        allowed_millis: i64,
    },
    #[error(
        "timestamp {ts_millis}ms is too old (now {now_millis}ms, allowed back to -{allowed_millis}ms)"
    )]
    TooOld {
        ts_millis: i64,
        now_millis: i64,
        allowed_millis: i64,
    },
}

pub fn needs_ctx_id(mqtt_topic: &str, fields: &HashMap<String, String>) -> bool {
    is_state_topic(mqtt_topic)
        && matches!(
            fields.get(STATE_KEY).map(String::as_str),
            Some(STATE_REQ_UNLOCK | STATE_LOCKED)
        )
}

pub fn normalize(
    mqtt_topic: &str,
    mut fields: HashMap<String, String>,
    ts_millis: Option<i64>,
    imei: &str,
    host_id: &str,
    last_ctx_id: Option<String>,
    limits: &TsLimits,
) -> Result<Vec<Routed>, DropReason> {
    if let Some(ts_millis) = ts_millis
        && let Err(reason) = validate_ts(ts_millis, limits)
    {
        tracing::error!(topic = %mqtt_topic, imei, ts_millis, reason = %reason, "invalid _ts");
    }

    let timestamp = ts_millis
        .and_then(DateTime::<Utc>::from_timestamp_millis)
        .unwrap_or_else(Utc::now)
        .to_rfc3339_opts(SecondsFormat::Millis, true);

    if is_gps_topic(mqtt_topic) && !merge_position(&mut fields) {
        tracing::error!(
            imei,
            ?fields,
            "missing latitude/longitude, dropping message"
        );
        return Err(DropReason::MissingPosition);
    }

    if is_state_topic(mqtt_topic) {
        apply_state(&mut fields, last_ctx_id);
    }

    Ok(fields
        .into_iter()
        .map(|(key, value)| Routed {
            topic: topic_for_key(&key),
            body: NormalizedMessage {
                id: Uuid::new_v4().to_string(),
                host_id: host_id.to_string(),
                imei: imei.to_string(),
                board_type: BOARD_TYPE.to_string(),
                timestamp: timestamp.clone(),
                payload: NormalizedMessagePayload { key, value },
            },
        })
        .collect())
}

pub fn validate_ts(ts_millis: i64, limits: &TsLimits) -> Result<(), TsError> {
    let now_millis = Utc::now().timestamp_millis();
    let max_after_millis = to_millis(limits.max_after_now_secs);
    let max_before_millis = to_millis(limits.max_before_now_secs);

    if ts_millis > now_millis.saturating_add(max_after_millis) {
        return Err(TsError::TooNew {
            ts_millis,
            now_millis,
            allowed_millis: max_after_millis,
        });
    }
    if ts_millis < now_millis.saturating_sub(max_before_millis) {
        return Err(TsError::TooOld {
            ts_millis,
            now_millis,
            allowed_millis: max_before_millis,
        });
    }
    Ok(())
}

fn to_millis(secs: u64) -> i64 {
    i64::try_from(secs)
        .unwrap_or(i64::MAX)
        .saturating_mul(1_000)
}

fn topic_for_key(key: &str) -> Topic {
    match key {
        EVENT_ID_KEY | CURRENT_STATE_KEY => Topic::Events,
        _ => Topic::Data,
    }
}

fn is_gps_topic(topic: &str) -> bool {
    topic
        .rsplit('/')
        .next()
        .and_then(|last| last.strip_prefix("gps"))
        .is_some_and(|rest| rest.chars().all(|c| c.is_ascii_digit()))
}

fn is_state_topic(topic: &str) -> bool {
    topic.rsplit('/').next() == Some("state")
}

fn apply_state(fields: &mut HashMap<String, String>, last_ctx_id: Option<String>) {
    let state = fields.get(STATE_KEY).map(String::as_str);
    let locked = state == Some(STATE_LOCKED);
    if !locked && state != Some(STATE_REQ_UNLOCK) {
        return;
    }

    if let Some(ctx_id) = last_ctx_id {
        fields.insert(LAST_CTX_ID_KEY.to_string(), ctx_id);
    }
    if locked {
        fields.insert(CTX_ID_KEY.to_string(), "-".to_string());
    }
}

fn merge_position(fields: &mut HashMap<String, String>) -> bool {
    let (Some(lat), Some(lon)) = (fields.get(LATITUDE_KEY), fields.get(LONGITUDE_KEY)) else {
        return false;
    };
    let position = format!("{lat},{lon}");

    fields.remove(LATITUDE_KEY);
    fields.remove(LONGITUDE_KEY);
    fields.insert(POSITION_KEY.to_string(), position);
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMITS: TsLimits = TsLimits {
        max_after_now_secs: 300,
        max_before_now_secs: 86_400,
    };
    const TS: i64 = 1_700_000_000_000;

    fn fields(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    fn run(topic: &str, pairs: &[(&str, &str)], last_ctx_id: Option<&str>) -> Vec<Routed> {
        normalize(
            topic,
            fields(pairs),
            Some(TS),
            "imei-1",
            "host-1",
            last_ctx_id.map(str::to_string),
            &LIMITS,
        )
        .expect("normalize should not drop this message")
    }

    fn value_of<'a>(routed: &'a [Routed], key: &str) -> Option<&'a str> {
        routed
            .iter()
            .find(|r| r.body.payload.key == key)
            .map(|r| r.body.payload.value.as_str())
    }

    #[test]
    fn builds_one_message_per_field_with_a_shared_envelope() {
        let routed = run("ccpm/imei-1/data", &[("speed", "42"), ("rpm", "900")], None);

        assert_eq!(routed.len(), 2);
        assert_eq!(value_of(&routed, "speed"), Some("42"));
        for r in &routed {
            assert_eq!(r.body.imei, "imei-1");
            assert_eq!(r.body.host_id, "host-1");
            assert_eq!(r.body.board_type, BOARD_TYPE);
            assert_eq!(r.body.timestamp, "2023-11-14T22:13:20.000Z");
        }
    }

    #[test]
    fn event_keys_route_to_the_events_stream_and_the_rest_to_data() {
        let routed = run(
            "ccpm/imei-1/data",
            &[("event_id", "7"), ("current_state", "on"), ("speed", "42")],
            None,
        );

        let topic_of = |key: &str| {
            routed
                .iter()
                .find(|r| r.body.payload.key == key)
                .map(|r| r.topic)
        };
        assert_eq!(topic_of("event_id"), Some(Topic::Events));
        assert_eq!(topic_of("current_state"), Some(Topic::Events));
        assert_eq!(topic_of("speed"), Some(Topic::Data));
    }

    #[test]
    fn gps_topic_merges_latitude_and_longitude_into_position() {
        let routed = run(
            "ccpm/imei-1/gps01",
            &[("latitude", "41.15"), ("longitude", "-8.61")],
            None,
        );

        assert_eq!(routed.len(), 1);
        assert_eq!(value_of(&routed, "position"), Some("41.15,-8.61"));
    }

    #[test]
    fn gps_topic_without_both_coordinates_is_dropped() {
        let dropped = normalize(
            "ccpm/imei-1/gps01",
            fields(&[("latitude", "41.15")]),
            Some(TS),
            "imei-1",
            "host-1",
            None,
            &LIMITS,
        );

        assert!(matches!(dropped, Err(DropReason::MissingPosition)));
    }

    #[test]
    fn non_gps_topic_keeps_coordinates_as_separate_fields() {
        let routed = run(
            "ccpm/imei-1/data",
            &[("latitude", "41.15"), ("longitude", "-8.61")],
            None,
        );

        assert_eq!(routed.len(), 2);
        assert_eq!(value_of(&routed, "position"), None);
    }

    #[test]
    fn locked_state_records_last_ctx_id_and_clears_the_current_one() {
        let routed = run(
            "ccpm/imei-1/state",
            &[("state", "_ST_LOCKED_")],
            Some("ctx-9"),
        );

        assert_eq!(value_of(&routed, "last_ctx_id"), Some("ctx-9"));
        assert_eq!(value_of(&routed, "ctx_id"), Some("-"));
    }

    #[test]
    fn unlock_request_records_last_ctx_id_without_clearing() {
        let routed = run(
            "ccpm/imei-1/state",
            &[("state", "_REQ_UNLOCK_")],
            Some("ctx-9"),
        );

        assert_eq!(value_of(&routed, "last_ctx_id"), Some("ctx-9"));
        assert_eq!(value_of(&routed, "ctx_id"), None);
    }

    #[test]
    fn other_states_are_passed_through_untouched() {
        let routed = run("ccpm/imei-1/state", &[("state", "_ST_OPEN_")], None);

        assert_eq!(routed.len(), 1);
        assert_eq!(value_of(&routed, "state"), Some("_ST_OPEN_"));
    }

    #[test]
    fn ctx_id_is_only_fetched_for_lock_states_on_the_state_topic() {
        assert!(needs_ctx_id(
            "ccpm/imei-1/state",
            &fields(&[("state", "_ST_LOCKED_")])
        ));
        assert!(needs_ctx_id(
            "ccpm/imei-1/state",
            &fields(&[("state", "_REQ_UNLOCK_")])
        ));
        assert!(!needs_ctx_id(
            "ccpm/imei-1/state",
            &fields(&[("state", "_ST_OPEN_")])
        ));
        assert!(!needs_ctx_id(
            "ccpm/imei-1/data",
            &fields(&[("state", "_ST_LOCKED_")])
        ));
    }

    #[test]
    fn a_missing_timestamp_falls_back_to_now() {
        let routed = normalize(
            "ccpm/imei-1/data",
            fields(&[("speed", "42")]),
            None,
            "imei-1",
            "host-1",
            None,
            &LIMITS,
        )
        .expect("missing _ts is not a drop reason");

        let stamped = DateTime::parse_from_rfc3339(&routed[0].body.timestamp)
            .expect("fallback timestamp should be rfc3339");
        assert!(
            (Utc::now() - stamped.with_timezone(&Utc))
                .num_seconds()
                .abs()
                < 5
        );
    }

    #[test]
    fn timestamps_outside_the_configured_window_are_rejected() {
        let now = Utc::now().timestamp_millis();

        assert!(validate_ts(now, &LIMITS).is_ok());
        assert!(matches!(
            validate_ts(now + 600_000, &LIMITS),
            Err(TsError::TooNew { .. })
        ));
        assert!(matches!(
            validate_ts(now - 172_800_000, &LIMITS),
            Err(TsError::TooOld { .. })
        ));
    }
}
