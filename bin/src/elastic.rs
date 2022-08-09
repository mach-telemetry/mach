use std::sync::{atomic::AtomicUsize, Arc};

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

pub struct CreateIndexArgs {
    pub num_shards: usize,
    pub num_replicas: usize,
}

impl Default for CreateIndexArgs {
    fn default() -> Self {
        // The default settings used by ES:
        // https://www.elastic.co/guide/en/elasticsearch/reference/7.14/indices-create-index.html
        Self {
            num_shards: 1,
            num_replicas: 1,
        }
    }
}

impl CreateIndexArgs {
    fn into_body(self) -> JsonBody<serde_json::Value> {
        let body: JsonBody<_> = json!({
            "settings": {
                "index": {
                  "number_of_shards": self.num_shards,
                  "number_of_replicas": self.num_replicas,
                }
              }
        })
        .into();

        body
    }
}

#[derive(Default)]
pub struct IngestStats {
    pub num_flushed: AtomicUsize,
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
        let batch = self.batch.clone();
        let batch_sz = batch.len();
        self.batch.clear();
        let mut bulk_msg_body: Vec<JsonBody<_>> = Vec::with_capacity(batch_sz * 2);
        for item in batch.into_iter() {
            bulk_msg_body.push(json!({"index": {}}).into());
            bulk_msg_body.push(item.into());
        }
        let r = self
            .client
            .bulk(BulkParts::Index(self.index_name.as_str()))
            .body(bulk_msg_body)
            .send()
            .await;
        self.stats
            .num_flushed
            .fetch_add(batch_sz, std::sync::atomic::Ordering::SeqCst);
        r
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
