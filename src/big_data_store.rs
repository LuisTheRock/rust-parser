use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Client, TlsCertificates};
use std::collections::HashMap;

#[derive(Clone)]
pub struct BigDataStore {
    conn: ConnectionManager,
    addr: redis::ConnectionAddr,
}

impl BigDataStore {
    pub async fn new(redis_url: &str, ca_cert_pem: Option<&str>) -> anyhow::Result<Self> {
        let client = match ca_cert_pem {
            Some(pem) => {
                let certs = TlsCertificates {
                    client_tls: None,
                    root_cert: Some(pem.as_bytes().to_vec()),
                };
                Client::build_with_tls(require_tls_scheme(redis_url), certs)?
            }
            None => Client::open(redis_url)?,
        };
        let addr = client.get_connection_info().addr().clone();
        let conn = ConnectionManager::new(client).await?;
        tracing::info!(addr = %addr, "redis connection established");
        Ok(Self { conn, addr })
    }

    /// Readiness probe.
    ///
    /// # Errors
    ///
    /// Returns an error when the connection manager cannot reach redis.
    pub async fn ping(&self) -> redis::RedisResult<()> {
        let mut conn = self.conn.clone();
        redis::cmd("PING").query_async::<()>(&mut conn).await
    }

    pub async fn get_current_ctx_id(&self, identifier: &str) -> redis::RedisResult<String> {
        let mut conn = self.conn.clone();
        let key = big_data_key(identifier, "ctx_id");
        let hash: HashMap<String, String> = conn.hgetall(&key).await?;
        Ok(hash.get("value").cloned().unwrap_or_default())
    }
}

impl Drop for BigDataStore {
    fn drop(&mut self) {
        tracing::info!(addr = %self.addr, "redis connection closing");
    }
}

fn big_data_key(identifier: &str, field: &str) -> String {
    format!("big-data:{identifier}:{field}")
}

fn require_tls_scheme(url: &str) -> String {
    match url.strip_prefix("redis://") {
        Some(rest) => format!("rediss://{rest}"),
        None => url.to_string(),
    }
}
