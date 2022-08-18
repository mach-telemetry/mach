mod elastic;
mod prep_data;
mod utils;

use clap::*;
use elastic::{ESBatchedIndexClient, ESClientBuilder, ESIndexQuerier, IngestStats};
use elasticsearch::http::request::JsonBody;
use elasticsearch::BulkParts;
use lazy_static::lazy_static;
use std::sync::atomic::Ordering::SeqCst;
use std::{
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};
use tokio::{sync::Barrier, time::Instant};
use utils::timestamp_now_micros;

lazy_static! {
    static ref ARGS: Args = Args::parse();
    static ref SAMPLES: Vec<prep_data::ESSample> = prep_data::load_samples(ARGS.file_path.as_str())
        .into_iter()
        .map(|s| s.into())
        .collect();
    static ref INDEX_NAME: String = format!("test-data-{}", timestamp_now_micros());
    static ref INGESTION_STATS: Arc<IngestStats> = Arc::new(IngestStats::default());
    static ref INDEXED_COUNT: AtomicUsize = AtomicUsize::new(0);
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

async fn bench(
    barr: Arc<Barrier>,
    es_builder: ESClientBuilder,
    batch_size: usize,
    run_duration: Duration,
) {
    let mut client = ESBatchedIndexClient::<prep_data::ESSample>::new(
        es_builder.build().unwrap(),
        INDEX_NAME.to_string(),
        batch_size,
        INGESTION_STATS.clone(),
    );

    barr.wait().await;
    let start = Instant::now();
    'outer: loop {
        for item in SAMPLES.iter() {
            match client.ingest(item.clone()).await {
                elastic::IngestResponse::Batched => (),
                elastic::IngestResponse::Flushed(r) => match r {
                    Err(e) => println!("Flush error: {:?}", e),
                    Ok(r) => {
                        if !r.status_code().is_success() {
                            println!("Flush error {}", r.status_code().as_u16());
                        }
                    }
                },
            }
            if start.elapsed() > run_duration {
                break 'outer;
            }
        }
    }
    client.flush().await.unwrap();
    drop(client);
    barr.wait().await;
}

async fn progress_watcher(barr: Arc<Barrier>, es_builder: ESClientBuilder, run_duration: Duration) {
    let one_sec = Duration::from_secs(1);
    let client = ESIndexQuerier::new(es_builder.build().unwrap());
    barr.wait().await;
    let start = Instant::now();
    while start.elapsed() < run_duration {
        match client.query_index_doc_count(INDEX_NAME.as_str()).await {
            Err(e) => println!("{:?}", e),
            Ok(res) => {
                if res.is_some() {
                    INDEXED_COUNT.store(res.unwrap().try_into().unwrap(), SeqCst);
                }
            }
        }
        tokio::time::sleep(one_sec).await;
    }
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
            num_shards: 6,
            num_replicas: 1,
        })
        .await
        .unwrap();
    println!("index created; name: {}", INDEX_NAME.as_str());

    let run_duration = Duration::from_secs(ARGS.bench_dur_secs.try_into().unwrap());
    let barr = Arc::new(Barrier::new(ARGS.es_num_writers + 2));
    for _ in 0..ARGS.es_num_writers {
        let barr = barr.clone();
        let es_builder = elastic_builder.clone();
        let batch_size = ARGS.es_ingest_batch_size;
        let run_dur = run_duration.clone();
        tokio::spawn(async move {
            bench(barr, es_builder, batch_size, run_dur).await;
        });
    }

    let barr_clone = barr.clone();
    let es_builder = elastic_builder.clone();
    let run_dur = run_duration.clone();
    tokio::spawn(async move {
        progress_watcher(barr_clone, es_builder, run_duration).await;
    });

    barr.wait().await;
    let one_sec = Duration::from_secs(1);
    let start = Instant::now();
    while start.elapsed() < run_duration {
        let num_flushed = INGESTION_STATS.num_flushed.load(SeqCst);
        let num_indexed = INDEXED_COUNT.load(SeqCst);
        let fraction_indexed: f64 = num_indexed as f64 / num_flushed as f64;
        println!("num flushed: {num_flushed}, num indexed: {num_indexed}, fraction indexed: {fraction_indexed}");
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
