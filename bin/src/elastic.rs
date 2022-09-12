use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::{atomic::AtomicUsize, Arc};
use std::time::Duration;
use tokio::time::Instant;

use elasticsearch::{
    auth::Credentials,
    http::{
        headers::HeaderMap,
        request::JsonBody,
        transport::{SingleNodeConnectionPool, Transport, TransportBuilder},
        Method, Url,
    },
    BulkParts, CountParts, Elasticsearch, SearchParts,
};
use serde_json::json;

use crate::utils::ExponentialBackoff;

#[derive(Default, Clone)]
pub struct ESClientBuilder {
    username: Option<String>,
    password: Option<String>,
    es_endpoint: Option<String>,
}

impl ESClientBuilder {
    pub fn username_optional(mut self, username: Option<String>) -> Self {
        self.username = username;
        self
    }

    pub fn password_optional(mut self, password: Option<String>) -> Self {
        self.password = password;
        self
    }

    pub fn endpoint(mut self, endpoint: String) -> Self {
        self.es_endpoint = Some(endpoint);
        self
    }

    pub fn build(self) -> Option<Elasticsearch> {
        if self.es_endpoint.is_none() {
            return Some(Elasticsearch::default());
        }
        if self.username.is_none() || self.password.is_none() {
            let transport = Transport::single_node(&self.es_endpoint?).ok()?;
            return Some(Elasticsearch::new(transport));
        }

        let url = Url::parse(self.es_endpoint?.as_str()).ok()?;
        let conn_pool = SingleNodeConnectionPool::new(url);
        let transport = TransportBuilder::new(conn_pool)
            .disable_proxy()
            .auth(Credentials::Basic(self.username?, self.password?))
            .build()
            .ok()?;
        let client = Elasticsearch::new(transport);
        Some(client)
    }
}

// A not-comprehensive list of data types supported by ES.
//
// See: https://www.elastic.co/guide/en/elasticsearch/reference/7.14/mapping-types.html
pub enum ESFieldType {
    Boolean,
    Binary,
    Long,
    Integer,
    UnsignedLong,
}

impl Into<&'static str> for ESFieldType {
    fn into(self) -> &'static str {
        match self {
            ESFieldType::Boolean => "boolean",
            ESFieldType::Binary => "binary",
            ESFieldType::Long => "long",
            ESFieldType::Integer => "integer",
            ESFieldType::UnsignedLong => "unsigned_long",
        }
    }
}

impl Into<serde_json::Value> for ESFieldType {
    fn into(self) -> serde_json::Value {
        let s: &'static str = self.into();
        serde_json::to_value(s).unwrap().into()
    }
}

pub struct CreateIndexArgs {
    pub num_shards: usize,
    pub num_replicas: usize,
    pub schema: Option<HashMap<String, ESFieldType>>,
}

impl Default for CreateIndexArgs {
    fn default() -> Self {
        // The default settings used by ES:
        // https://www.elastic.co/guide/en/elasticsearch/reference/7.14/indices-create-index.html
        Self {
            num_shards: 1,
            num_replicas: 1,
            schema: None,
        }
    }
}

impl CreateIndexArgs {
    fn into_body(self) -> JsonBody<serde_json::Value> {
        let mut body = json!({
            "settings": {
                "index": {
                  "number_of_shards": self.num_shards,
                  "number_of_replicas": self.num_replicas,
                }
              }
        });

        if let Some(schema) = self.schema {
            for (k, v) in schema.into_iter() {
                body["mappings"]["properties"][k]["type"] = v.into();
            }
        }

        body.into()
    }
}

#[derive(Default)]
pub struct IngestStats {
    pub num_flushed: AtomicUsize,
    pub num_flush_reqs_initiated: AtomicUsize,
    pub num_flush_reqs_completed: AtomicUsize,
    pub flush_dur_millis: AtomicU64,
}

pub type ESResponse = Result<elasticsearch::http::response::Response, elasticsearch::Error>;

pub enum IngestResponse {
    Batched,
    Flushed(ESResponse),
}

pub struct ESBatchedIndexClient<T: Clone + Into<JsonBody<serde_json::Value>>> {
    client: Elasticsearch,
    batch: Vec<T>,
    batch_size: usize,
    index_name: String,
    backoff: ExponentialBackoff,
    max_retries: usize,
    stats: Arc<IngestStats>,
}

impl<T: Clone + Into<JsonBody<serde_json::Value>>> Drop for ESBatchedIndexClient<T> {
    fn drop(&mut self) {
        assert!(self.batch.len() == 0, "Some samples were not flushed");
    }
}

impl<T: Clone + Into<JsonBody<serde_json::Value>>> ESBatchedIndexClient<T> {
    pub fn new(
        client: Elasticsearch,
        index_name: String,
        batch_size: usize,
        stats: Arc<IngestStats>,
    ) -> Self {
        Self {
            client,
            batch: Vec::with_capacity(batch_size),
            batch_size,
            index_name,
            backoff: ExponentialBackoff::new(Duration::from_secs(1), Duration::from_secs(5)),
            max_retries: 3,
            stats,
        }
    }

    pub async fn ingest(&mut self, doc: T) -> IngestResponse {
        self.batch.push(doc);
        match self.batch.len() == self.batch_size {
            true => IngestResponse::Flushed(self.flush().await),
            false => IngestResponse::Batched,
        }
    }

    pub async fn flush(&mut self) -> ESResponse {
        self.stats.num_flush_reqs_initiated.fetch_add(1, SeqCst);
        let mut num_retries = 0;
        loop {
            let batch_sz = self.batch.len();
            let mut bulk_msg_body: Vec<JsonBody<_>> = Vec::with_capacity(batch_sz * 2);
            for item in self.batch.iter() {
                bulk_msg_body.push(json!({"index": {}}).into());
                bulk_msg_body.push(item.clone().into());
            }

            let start = Instant::now();
            let r = self
                .client
                .bulk(BulkParts::Index(self.index_name.as_str()))
                .body(bulk_msg_body)
                .send()
                .await;
            let bulk_dur = start.elapsed();

            if let Ok(ref r) = r {
                if r.status_code().is_success() {
                    self.stats.num_flushed.fetch_add(batch_sz, SeqCst);
                } else if num_retries < self.max_retries && r.status_code().as_u16() == 429 {
                    // too-many-requests
                    tokio::time::sleep(self.backoff.next_backoff()).await;
                    num_retries += 1;
                    continue;
                }
            }
            self.batch.clear();
            self.backoff.reset();
            self.stats
                .flush_dur_millis
                .fetch_add(bulk_dur.as_millis().try_into().unwrap(), SeqCst);
            self.stats.num_flush_reqs_completed.fetch_add(1, SeqCst);
            return r;
        }
    }

    pub async fn create_index(&self, args: CreateIndexArgs) -> ESResponse {
        let body = Some(args.into_body());
        let query_string = None;
        self.client
            .send::<JsonBody<_>, String>(
                Method::Put,
                format!("/{}", self.index_name.as_str()).as_str(),
                HeaderMap::new(),
                query_string,
                body,
                None,
            )
            .await
    }
}

pub struct ESIndexQuerier {
    client: Elasticsearch,
}

impl ESIndexQuerier {
    pub fn new(client: Elasticsearch) -> Self {
        Self { client }
    }

    pub async fn query_latest_timestamp(
        &self,
        index_name: &str,
    ) -> Result<Option<u64>, elasticsearch::Error> {
        let r = self
            .client
            .search(SearchParts::Index(&[index_name]))
            .sort(&["timestamp:desc"])
            .size(1)
            .body(json!({
                "query": {
                    "match_all": {}
                }
            }))
            .send()
            .await?;

        let r_body = r
            .json::<serde_json::Value>()
            .await
            .expect("Failed to parse response as json");
        match &r_body["hits"]["hits"][0]["sort"][0] {
            serde_json::Value::Null => Ok(None),
            serde_json::Value::Number(ts) => Ok(Some(ts.as_u64().unwrap())),
            _ => unreachable!("unexpected timestamp type"),
        }
    }

    pub async fn query_index_doc_count(
        &self,
        index_name: &str,
    ) -> Result<Option<u64>, elasticsearch::Error> {
        let r = self
            .client
            .count(CountParts::Index(&[index_name]))
            .body(json!({"query": { "match_all": {} }}))
            .send()
            .await?;
        let r_body = r
            .json::<serde_json::Value>()
            .await
            .expect("Failed to parse response as json");
        match &r_body["count"] {
            serde_json::Value::Null => Ok(None),
            serde_json::Value::Number(doc_count) => Ok(Some(doc_count.as_u64().unwrap())),
            _ => unreachable!("unexpected doc count type"),
        }
    }
}
