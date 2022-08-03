use crate::completeness::{
    kafka::{decompress_kafka_msg, kafka_writer},
    SampleOwned, Writer,
};
use crate::{kafka_utils::make_topic, prep_data, utils::timestamp_now_micros};
use crossbeam_channel::{bounded, Receiver, Sender};
use elasticsearch::{http::request::JsonBody, BulkParts, Elasticsearch, IndexParts, SearchParts};
use kafka::{client::KafkaClient, consumer::Consumer};
use lazy_static::lazy_static;
use mach::{id::SeriesId, sample::SampleType};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::{Arc, Barrier};
use std::thread;

lazy_static! {
    static ref ES_INDEX_NAME: String = format!("test-data-{}", timestamp_now_micros());
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
    fn new(index_name: String, batch_size: usize) -> Self {
        Self {
            client: Elasticsearch::default(),
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
    index_name: &String,
    consumer: Receiver<Vec<ESSample>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let batch_size = 1024;
    let mut client = ESBatchedIndexClient::new(index_name.clone(), batch_size);

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

fn es_ingestor(index_name: &String, consumer: Receiver<Vec<ESSample>>) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        es_ingest(index_name, consumer).await.unwrap();
    });
}

async fn es_watch_freshness(index_name: &String) {
    let one_sec = std::time::Duration::from_secs(1);
    let client = Elasticsearch::default();
    loop {
        let r = client
            .search(SearchParts::Index(&[index_name.as_str()]))
            .sort(&["timestamp:desc"])
            .size(1)
            .body(json!({
                "query": {
                    "match_all": {}
                }
            }))
            .send()
            .await
            .unwrap();
        let r_body = r.json::<serde_json::Value>().await.unwrap();
        match &r_body["hits"]["hits"][0]["sort"][0] {
            serde_json::Value::Null => println!("es query erred; index not ready yet?"),
            serde_json::Value::Number(ts) => {
                let ts_curr = timestamp_now_micros();
                let ts_got = ts.as_u64().unwrap();
                println!("got ts {}, freshness: {}", ts_got, ts_curr - ts_got);
            }
            _ => unreachable!("unexpected timestamp type"),
        }
        tokio::time::sleep(one_sec).await;
    }
}

fn es_freshness_watcher(index_name: &String) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        es_watch_freshness(index_name).await;
    });
}

pub fn init_kafka_es(
    kafka_bootstraps: &'static str,
    kafka_topic: &'static str,
    num_writers: usize,
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

    let (consume_tx, consume_rx) = bounded(10);
    thread::spawn(move || kafka_es_consumer(&kafka_topic, &kafka_bootstraps, consume_tx));
    thread::spawn(move || es_ingestor(&ES_INDEX_NAME, consume_rx));
    thread::spawn(move || es_freshness_watcher(&ES_INDEX_NAME));

    Writer {
        sender: tx,
        barrier,
    }
}
