use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct NormalizedMessagePayload {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Serialize)]
pub struct NormalizedMessage {
    pub id: String,
    #[serde(rename = "hostId")]
    pub host_id: String,
    pub imei: String,
    #[serde(rename = "boardType")]
    pub board_type: String,
    pub timestamp: String,
    pub payload: NormalizedMessagePayload,
}
