mod prep_data;

use std::{
    fmt::format,
    sync::atomic::AtomicBool,
    sync::atomic::Ordering,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use clap::*;
use elasticsearch::{http::request::JsonBody, BulkParts, Elasticsearch, IndexParts, SearchParts};
use lazy_static::lazy_static;
use mach::{id::SeriesId, sample::SampleType};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Barrier;

const BATCH_SIZE: usize = 1024;

lazy_static! {
    static ref ARGS: Args = Args::parse();
    static ref INDEX_NAME: String = format!("test-data-{}", timestamp_now());
    static ref COMPLETED: AtomicBool = AtomicBool::new(false);
}

#[derive(Serialize, Deserialize, Clone)]
struct ESSample {
    series_id: SeriesId,
    timestamp: u64,
    data: Vec<SampleType>,
}

impl From<prep_data::Sample> for ESSample {
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

fn timestamp_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}

async fn es_ingestor(index_name: &String, samples: Vec<ESSample>, start_barr: Arc<Barrier>) {
    let client = Elasticsearch::default();

    client
        .index(IndexParts::Index(index_name))
        .send()
        .await
        .unwrap();
    println!("index {} created", index_name);

    start_barr.wait().await;

    println!("start writing to ES index {}", index_name);

    let mut batch_item_no = 0;
    let mut num_batches_written = 0;
    let mut body: Vec<JsonBody<_>> = Vec::with_capacity(BATCH_SIZE * 2);
    for mut sample in samples {
        sample.timestamp = timestamp_now();
        body.push(json!({"index": {"_id": batch_item_no}}).into());
        body.push(sample.into());
        batch_item_no += 1;
        if batch_item_no == BATCH_SIZE {
            let r = client
                .bulk(BulkParts::Index(index_name.as_str()))
                .body(body)
                .send()
                .await
                .unwrap();
            assert!(r.error_for_status_code().is_ok());
            batch_item_no = 0;
            num_batches_written += 1;
            println!("Num batches written: {}", num_batches_written);
            body = Vec::with_capacity(BATCH_SIZE * 2);
        }
    }

    COMPLETED.store(true, Ordering::SeqCst);
}

async fn es_watch_freshness(index_name: &String, start_barr: Arc<Barrier>) {
    let one_sec = std::time::Duration::from_secs(1);
    let client = Elasticsearch::default();

    start_barr.wait().await;

    while !COMPLETED.load(Ordering::SeqCst) {
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
        let r_body = r.json::<Value>().await.unwrap();
        match &r_body["hits"]["hits"][0]["sort"][0] {
            Value::Null => println!("index not ready yet?"),
            Value::Number(ts) => {
                let ts_curr = timestamp_now();
                let ts_got = ts.as_u64().unwrap();
                println!("got ts {}, freshness: {}", ts_got, ts_curr - ts_got);
            },
            _ => unreachable!("unexpected timestamp type")
        }
        std::thread::sleep(one_sec);
    }
}

#[derive(Parser, Debug)]
struct Args {
    #[clap(short, long, default_value_t = String::from("/home/sli/data/train-ticket-data"))]
    file_path: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let samples: Vec<ESSample> = prep_data::load_samples(ARGS.file_path.as_str())
        .into_iter()
        .map(|s| s.into())
        .collect();

    let start_barr = Arc::new(Barrier::new(2));
    let b1 = start_barr.clone();
    let b2 = start_barr.clone();
    let ingestor = tokio::spawn(async { es_ingestor(&INDEX_NAME, samples, b1).await });
    let freshness_watcher = tokio::spawn(async { es_watch_freshness(&INDEX_NAME, b2).await });

    ingestor.await.unwrap();
    freshness_watcher.await.unwrap();

    Ok(())
}
