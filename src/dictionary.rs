use anyhow::Context;
use serde::Deserialize;
use std::time::Duration;

const PAGE_SIZE: u32 = 200;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Deserialize)]
struct Key {
    #[serde(rename = "rawKey")]
    raw_key: String,
}

#[derive(Deserialize)]
struct ListKeysResponse {
    keys: Vec<Key>,
}

pub struct DictionaryClient {
    base_url: String,
    http: reqwest::Client,
}

impl DictionaryClient {
    pub fn new(base_url: &str) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()?;
        Ok(Self {
            base_url: base_url.to_string(),
            http,
        })
    }

    pub async fn fetch_keys(&self, board_type: &str) -> anyhow::Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut page = 1u32;
        loop {
            let page_keys = self.fetch_page(board_type, page).await?;
            let got = page_keys.len();
            keys.extend(page_keys);
            if got < PAGE_SIZE as usize {
                break;
            }
            page += 1;
        }
        Ok(keys)
    }

    async fn fetch_page(&self, board_type: &str, page: u32) -> anyhow::Result<Vec<String>> {
        let url = format!("{}/service-dictionary-normalizer/v1/keys", self.base_url);
        let response = self
            .http
            .get(&url)
            .query(&[
                ("boardType", board_type),
                ("pageSize", &PAGE_SIZE.to_string()),
                ("page", &page.to_string()),
            ])
            .send()
            .await
            .with_context(|| format!("fetching dictionary keys (page {page})"))?;

        if response.status() == reqwest::StatusCode::NO_CONTENT {
            return Ok(Vec::new());
        }
        if !response.status().is_success() {
            anyhow::bail!(
                "fetching dictionary keys (page {page}): unexpected status {}",
                response.status()
            );
        }

        let body: ListKeysResponse = response
            .json()
            .await
            .with_context(|| format!("decoding dictionary keys response (page {page})"))?;

        Ok(body.keys.into_iter().map(|k| k.raw_key).collect())
    }
}
