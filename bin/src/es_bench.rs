mod elastic;
mod prep_data;
mod utils;

use clap::*;
use elastic::{ESBatchedIndexClient, ESClientBuilder, IngestStats};
use lazy_static::lazy_static;
use std::{sync::Arc, time::Duration};
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
            client.ingest(item.clone()).await;
            if start.elapsed() > run_duration {
                break 'outer;
            }
        }
    }
    client.flush().await.unwrap();
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

    let run_duration = Duration::from_secs(ARGS.bench_dur_secs.try_into().unwrap());
    let barr = Arc::new(Barrier::new(ARGS.es_num_writers + 1));
    for _ in 0..ARGS.es_num_writers {
        let barr = barr.clone();
        let es_builder = elastic_builder.clone();
        let batch_size = ARGS.es_ingest_batch_size;
        let run_dur = run_duration.clone();
        tokio::spawn(async move {
            bench(barr, es_builder, batch_size, run_dur);
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
    println!("samples per sec: {samples_per_sec}");
}
