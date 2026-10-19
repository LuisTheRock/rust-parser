use serde_json::Value;
use std::collections::HashMap;

pub fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => match n.as_i64() {
            Some(i) => i.to_string(),
            None => format!("{:.6}", n.as_f64().unwrap_or(0.0)),
        },
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

pub fn map_to_string_map(data: &serde_json::Map<String, Value>) -> HashMap<String, String> {
    data.iter()
        .filter_map(|(k, v)| {
            if v.is_null() {
                tracing::info!(key = %k, "dropping null field");
                None
            } else {
                Some((k.clone(), value_to_string(v)))
            }
        })
        .collect()
}
