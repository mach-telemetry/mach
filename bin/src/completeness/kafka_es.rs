use crate::completeness::{
    kafka::{decompress_kafka_msg, kafka_writer},
    SampleOwned, Writer,
};
use crate::{kafka_utils::make_topic, prep_data, utils::timestamp_now_micros};
use crossbeam_channel::{bounded, Receiver, Sender};
use elasticsearch::{
    auth::Credentials,
    http::{
        request::JsonBody,
        transport::{SingleNodeConnectionPool, TransportBuilder},
        Url,
    },
    BulkParts, Elasticsearch, IndexParts, SearchParts,
};
use kafka::{client::KafkaClient, consumer::Consumer};
use lazy_static::lazy_static;
use mach::{id::SeriesId, sample::SampleType};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

lazy_static! {
    static ref ES_INDEX_NAME: String = format!("test-data-{}", timestamp_now_micros());
}

#[derive(Default, Clone)]
pub struct ESClientBuilder {
    username: Option<String>,
    password: Option<String>,
    es_endpoint: Option<String>,
}

impl ESClientBuilder {
    pub fn username(mut self, username: String) -> Self {
        self.username = Some(username);
        self
    }

    pub fn password(mut self, password: String) -> Self {
        self.password = Some(password);
        self
    }

    pub fn endpoint(mut self, endpoint: String) -> Self {
        self.es_endpoint = Some(endpoint);
        self
    }

    pub fn build(self) -> Option<Elasticsearch> {
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

#[derive(Serialize, Deserialize, Clone)]
pub struct ESSample {
    series_id: SeriesId,
    timestamp: u64,
    data: Vec<SampleType>,
}

impl From<SampleOwned<SeriesId>> for ESSample {
    fn from(data: prep_data::Sample) -> ESSample {
        ESSample {
            series_id: data.0,
            timestamp: data.1,
            data: data.2,
        }
    }
}

impl Into<JsonBody<serde_json::Value>> for ESSample {
    fn into(self) -> JsonBody<serde_json::Value> {
        serde_json::to_value(self).unwrap().into()
    }
}

type ESResponse = Result<elasticsearch::http::response::Response, elasticsearch::Error>;

enum IngestResponse {
    Batched,
    Flushed(ESResponse),
}

struct ESBatchedIndexClient {
    client: Elasticsearch,
    batch: Vec<ESSample>,
    batch_size: usize,
    index_name: String,
    index_created: bool,
}

impl Drop for ESBatchedIndexClient {
    fn drop(&mut self) {
        assert!(self.batch.len() == 0, "Some samples were not flushed");
    }
}

impl ESBatchedIndexClient {
    fn new(client: Elasticsearch, index_name: String, batch_size: usize) -> Self {
        Self {
            client,
            batch: Vec::with_capacity(batch_size),
            batch_size,
            index_name,
            index_created: false,
        }
    }

    async fn ingest(&mut self, doc: ESSample) -> IngestResponse {
        self.batch.push(doc);
        match self.batch.len() == self.batch_size {
            true => IngestResponse::Flushed(self.flush().await),
            false => IngestResponse::Batched,
        }
    }

    async fn flush(&mut self) -> ESResponse {
        if !self.index_created {
            self._create_index(self.index_name.as_str())
                .await
                .expect("index creation failed");
            self.index_created = true;
        }

        let batch = self.batch.clone();
        self.batch.clear();
        let mut bulk_msg_body: Vec<JsonBody<_>> = Vec::with_capacity(self.batch_size * 2);
        for (item_no, item) in batch.into_iter().enumerate() {
            bulk_msg_body.push(json!({"index": {"_id": item_no}}).into());
            bulk_msg_body.push(item.into());
        }

        self.client
            .bulk(BulkParts::Index(self.index_name.as_str()))
            .body(bulk_msg_body)
            .send()
            .await
    }

    async fn _create_index(&self, name: &str) -> ESResponse {
        self.client.index(IndexParts::Index(name)).send().await
    }
}

struct ESIndexQuerier {
    client: Elasticsearch,
}

impl ESIndexQuerier {
    fn new(client: Elasticsearch) -> Self {
        Self { client }
    }

    async fn query_latest_timestamp(
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
}

fn kafka_es_consumer(topic: &str, bootstraps: &str, sender: Sender<Vec<ESSample>>) {
    let mut kafka_client = KafkaClient::new(bootstraps.split(',').map(String::from).collect());
    kafka_client.load_metadata_all().unwrap();
    let mut kafka_consumer = Consumer::from_client(kafka_client)
        .with_topic(String::from(topic))
        .with_fetch_max_bytes_per_partition(10_000_000)
        .create()
        .unwrap();

    let mut buffer = vec![0u8; 500_000_000];
    loop {
        for ms in kafka_consumer.poll().unwrap().iter() {
            for msg in ms.messages().iter() {
                let data = decompress_kafka_msg(msg, buffer.as_mut_slice());
                let es_data: Vec<ESSample> = data.into_iter().map(|s| s.into()).collect();
                sender.send(es_data).unwrap();
            }
        }
    }
}

async fn es_ingest(
    mut client: ESBatchedIndexClient,
    consumer: Receiver<Vec<ESSample>>,
) -> Result<(), Box<dyn std::error::Error>> {
    while let Ok(samples) = consumer.recv() {
        for sample in samples {
            match client.ingest(sample).await {
                IngestResponse::Batched => {}
                IngestResponse::Flushed(r) => {
                    assert!(r
                        .expect("could not submit samples to ES")
                        .error_for_status_code()
                        .is_ok());
                }
            }
        }
    }

    let r = client.flush().await.unwrap();
    assert!(r.error_for_status_code().is_ok());

    Ok(())
}

fn es_ingestor(
    es_client_builder: ESClientBuilder,
    index_name: &String,
    ingest_batch_size: usize,
    consumer: Receiver<Vec<ESSample>>,
) {
    let client = ESBatchedIndexClient::new(
        es_client_builder.build().unwrap(),
        index_name.to_string(),
        ingest_batch_size,
    );
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        es_ingest(client, consumer).await.unwrap();
    });
}

async fn es_watch_freshness(client: ESIndexQuerier, index_name: &String) {
    let one_sec = std::time::Duration::from_secs(1);
    loop {
        match client.query_latest_timestamp(index_name).await {
            Ok(resp) => match resp {
                None => println!("index not ready yet"),
                Some(ts_got) => {
                    let ts_curr = timestamp_now_micros();
                    let delta = Duration::from_micros(ts_curr - ts_got);
                    println!("got ts {}, freshness: {}", ts_got, delta.as_secs_f64());
                }
            },
            Err(e) => {
                println!("query latest timestamp err: {:?}", e);
            }
        }
        tokio::time::sleep(one_sec).await;
    }
}

fn es_freshness_watcher(es_client_builder: ESClientBuilder, index_name: &String) {
    let querier = ESIndexQuerier::new(es_client_builder.build().unwrap());
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        es_watch_freshness(querier, index_name).await;
    });
}

pub fn init_kafka_es(
    kafka_bootstraps: &'static str,
    kafka_topic: &'static str,
    num_writers: usize,
    es_client_builder: ESClientBuilder,
    es_ingest_batch_size: usize,
) -> Writer<SeriesId> {
    make_topic(&kafka_bootstraps, &kafka_topic);
    let (tx, rx) = bounded(1);
    let barrier = Arc::new(Barrier::new(num_writers + 1));

    for _ in 0..num_writers {
        let barrier = barrier.clone();
        let rx = rx.clone();
        thread::spawn(move || {
            kafka_writer(kafka_bootstraps, kafka_topic, barrier, rx);
        });
    }

    let ingestor_es_builder = es_client_builder.clone();
    let (consume_tx, consume_rx) = bounded(10);
    thread::spawn(move || kafka_es_consumer(kafka_topic, &kafka_bootstraps, consume_tx));
    thread::spawn(move || {
        es_ingestor(
            ingestor_es_builder,
            &ES_INDEX_NAME,
            es_ingest_batch_size,
            consume_rx,
        )
    });
    thread::spawn(move || es_freshness_watcher(es_client_builder, &ES_INDEX_NAME));

    Writer {
        sender: tx,
        barrier,
    }
}
