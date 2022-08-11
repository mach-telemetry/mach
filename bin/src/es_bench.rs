mod elastic;
mod prep_data;
mod utils;

use clap::*;
use elastic::{ESBatchedIndexClient, ESClientBuilder, IngestStats};
use elasticsearch::http::request::JsonBody;
use elasticsearch::BulkParts;
use lazy_static::lazy_static;
use std::{sync::atomic::Ordering::SeqCst, sync::Arc, time::Duration};
use tokio::{sync::Barrier, time::Instant};
use utils::timestamp_now_micros;

lazy_static! {
    static ref ARGS: Args = Args::parse();
    static ref SAMPLES: Vec<String> = prep_data::load_samples(ARGS.file_path.as_str())
        .drain(..)
        .map(|s: prep_data::Sample| {
            let s: prep_data::ESSample = s.into();
            serde_json::to_string(&s).unwrap()
        })
        .collect();
    static ref INDEX_NAME: String = format!("test-data-{}", timestamp_now_micros());
    static ref INGESTION_STATS: Arc<IngestStats> = Arc::new(IngestStats::default());
}

#[derive(Parser, Debug, Clone)]
struct Args {
    #[clap(short, long, default_value_t = String::from("/home/sli/data/train-ticket-data"))]
    file_path: String,

    #[clap(short, long, default_value_t = String::from("http://localhost:9200"))]
    es_endpoint: String,

    #[clap(short, long)]
    es_username: Option<String>,

    #[clap(short, long)]
    es_password: Option<String>,

    #[clap(short, long, default_value_t = 1024)]
    es_ingest_batch_size: usize,

    #[clap(short, long, default_value_t = 40)]
    es_num_writers: usize,

    #[clap(short, long, default_value_t = 120)]
    bench_dur_secs: usize,
}

async fn flush(client: &mut elasticsearch::Elasticsearch, batch: Vec<JsonBody<String>>) {
    match client
        .bulk(BulkParts::Index(&INDEX_NAME))
        .body(batch)
        .send()
        .await {
            Ok(_) => {},
            Err(e) => {
                println!("flush error: {:?}", e);
            }
        }
}

async fn bench(
    barr: Arc<Barrier>,
    es_builder: ESClientBuilder,
    batch_size: usize,
    run_duration: Duration,
) {
    let mut client = es_builder.build().unwrap();
    let mut batch = Vec::with_capacity(batch_size * 2);

    barr.wait().await;
    let start = Instant::now();
    'outer: loop {
        for item in SAMPLES.iter() {
            batch.push(item.clone().into());
            if batch.len() == batch_size * 2 {
                flush(&mut client, batch).await;
                INGESTION_STATS.num_flushed.fetch_add(batch_size, SeqCst);
                batch = Vec::with_capacity(batch_size * 2);
            }
            if start.elapsed() > run_duration {
                break 'outer;
            }
        }
    }
    if batch.len() > 0 {
        let batch_len = batch.len();
        flush(&mut client, batch).await;
        INGESTION_STATS.num_flushed.fetch_add(batch_len, SeqCst);
    }
    drop(client);
    barr.wait().await;
}

#[tokio::main]
async fn main() {
    let _ = SAMPLES.len();

    let elastic_builder = ESClientBuilder::default()
        .endpoint(ARGS.es_endpoint.clone())
        .username_optional(ARGS.es_username.clone())
        .password_optional(ARGS.es_password.clone());

    let client = ESBatchedIndexClient::<prep_data::ESSample>::new(
        elastic_builder.clone().build().unwrap(),
        INDEX_NAME.clone(),
        ARGS.es_ingest_batch_size,
        INGESTION_STATS.clone(),
    );
    client
        .create_index(elastic::CreateIndexArgs {
            num_shards: 3,
            num_replicas: 1,
        })
        .await
        .unwrap();
    println!("index created");

    let run_duration = Duration::from_secs(ARGS.bench_dur_secs.try_into().unwrap());
    let barr = Arc::new(Barrier::new(ARGS.es_num_writers + 1));
    for _ in 0..ARGS.es_num_writers {
        let barr = barr.clone();
        let es_builder = elastic_builder.clone();
        let batch_size = ARGS.es_ingest_batch_size;
        let run_dur = run_duration.clone();
        tokio::spawn(async move {
            bench(barr, es_builder, batch_size, run_dur).await;
        });
    }
    barr.wait().await;

    let one_sec = Duration::from_secs(1);
    let start = Instant::now();
    while start.elapsed() < run_duration {
        let num_flushed = INGESTION_STATS
            .num_flushed
            .load(std::sync::atomic::Ordering::SeqCst);
        println!("num flushed: {num_flushed}");
        tokio::time::sleep(one_sec).await;
    }
    barr.wait().await;

    let bench_dur = start.elapsed();

    let num_flushed = INGESTION_STATS
        .num_flushed
        .load(std::sync::atomic::Ordering::SeqCst);
    let samples_per_sec = num_flushed as f64 / bench_dur.as_secs_f64();
    println!(
        "bench duration: {}, number of samples: {}, samples per sec: {samples_per_sec}",
        bench_dur.as_secs_f64(),
        num_flushed
    );
}
