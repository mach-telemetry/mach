mod completeness;
mod elastic;
mod kafka_utils;
mod prep_data;
mod utils;

use crate::completeness::kafka::decompress_kafka_msg;
use crate::prep_data::ESSample;
use clap::*;
use crossbeam_channel::{bounded, Receiver, Sender};
use elastic::{
    CreateIndexArgs, ESBatchedIndexClient, ESClientBuilder, ESFieldType, ESIndexQuerier,
    IngestResponse, IngestStats,
};
use kafka::{client::KafkaClient, consumer::Consumer};
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use utils::timestamp_now_micros;

lazy_static! {
    static ref ARGS: Args = Args::parse();
    static ref INDEX_NAME: String = format!("test-data-{}", timestamp_now_micros());
    static ref INGESTION_STATS: Arc<IngestStats> = Arc::new(IngestStats::default());
    static ref INDEXED_COUNT: AtomicU64 = AtomicU64::new(0);
}

#[derive(Parser, Debug, Clone)]
struct Args {
    #[clap(short, long, default_value_t = String::from("localhost:9093,localhost:9094,localhost:9095"))]
    kafka_bootstraps: String,

    #[clap(short, long, default_value_t = String::from("kafka-completeness-bench"))]
    kafka_topic: String,

    #[clap(short, long, default_value_t = String::from("http://localhost:9200"))]
    es_endpoint: String,

    #[clap(short, long)]
    es_username: Option<String>,

    #[clap(short, long)]
    es_password: Option<String>,

    #[clap(short, long, default_value_t = 5000)]
    es_ingest_batch_size: usize,

    #[clap(short, long, default_value_t = 40)]
    es_num_writers: usize,

    #[clap(short, long, default_value_t = 3)]
    es_num_shards: usize,

    #[clap(short, long, default_value_t = 0)]
    es_num_replicas: usize,
}

async fn es_watch_data_age(client: ESIndexQuerier, index_name: &String) {
    let one_sec = std::time::Duration::from_secs(1);
    loop {
        match client.query_latest_timestamp(index_name).await {
            Ok(resp) => match resp {
                None => (),
                Some(ts_got) => {
                    let ts_curr = timestamp_now_micros();
                    let delta = Duration::from_micros(ts_curr - ts_got);
                    println!("got ts {}, data age: {}", ts_got, delta.as_secs_f64());
                }
            },
            Err(e) => {
                println!("query latest timestamp err: {:?}", e);
            }
        }
        tokio::time::sleep(one_sec).await;
    }
}

fn es_data_age_watcher(es_client_builder: ESClientBuilder, index_name: &String) {
    let querier = ESIndexQuerier::new(es_client_builder.build().unwrap());
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        es_watch_data_age(querier, index_name).await;
    });
}

async fn es_watch_docs_count(client: ESIndexQuerier, index_name: &String) {
    let one_sec = std::time::Duration::from_secs(1);
    loop {
        match client.query_index_doc_count(index_name).await {
            Ok(resp) => match resp {
                None => (),
                Some(doc_count) => {
                    INDEXED_COUNT.store(doc_count, SeqCst);
                }
            },
            Err(e) => {
                println!("query doc count err: {:?}", e);
            }
        }
        tokio::time::sleep(one_sec).await;
    }
}

fn es_docs_count_watcher(es_client_builder: ESClientBuilder, index_name: &String) {
    let querier = ESIndexQuerier::new(es_client_builder.build().unwrap());
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        es_watch_docs_count(querier, index_name).await;
    });
}

fn kafka_es_consumer(topic: &str, bootstraps: &str, sender: Sender<Vec<ESSample>>) {
    let mut kafka_client = KafkaClient::new(bootstraps.split(',').map(String::from).collect());
    kafka_client.load_metadata_all().unwrap();
    let mut kafka_consumer = Consumer::from_client(kafka_client)
        .with_topic(String::from(topic))
        .with_fetch_max_bytes_per_partition(10_000_000)
        .create()
        .unwrap();

    println!(
        "Kafka consumer created, begin consuming from topic {}",
        topic
    );

    let mut buffer = vec![0u8; 500_000_000];
    loop {
        for ms in kafka_consumer.poll().unwrap().iter() {
            for msg in ms.messages().iter() {
                let (start, end, data) = decompress_kafka_msg(msg.value, buffer.as_mut_slice());
                let es_data: Vec<ESSample> = data.into_iter().map(|s| s.into()).collect();
                sender.send(es_data).unwrap();
            }
        }
    }
}

async fn es_ingest(
    mut client: ESBatchedIndexClient<ESSample>,
    consumer: Receiver<Vec<ESSample>>,
) -> Result<(), Box<dyn std::error::Error>> {
    while let Ok(samples) = consumer.recv() {
        for sample in samples {
            match client.ingest(sample).await {
                IngestResponse::Batched => {}
                IngestResponse::Flushed(r) => match r {
                    Ok(r) => {
                        if !r.status_code().is_success() {
                            println!("error resp: {:?}", r);
                        }
                    }
                    Err(e) => {
                        println!("error: {:?}", e);
                    }
                },
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
    num_ingestors: usize,
    consumer: Receiver<Vec<ESSample>>,
) {
    let builder_clone = es_client_builder.clone();
    let client = ESBatchedIndexClient::<ESSample>::new(
        builder_clone.build().unwrap(),
        index_name.to_string(),
        ingest_batch_size,
        INGESTION_STATS.clone(),
    );
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let mut handles = Vec::new();
        for _ in 0..num_ingestors {
            let builder_clone = es_client_builder.clone();
            let index = index_name.clone();
            let consumer = consumer.clone();
            let batch_size = ingest_batch_size;
            handles.push(tokio::spawn(async move {
                let client = ESBatchedIndexClient::new(
                    builder_clone.build().unwrap(),
                    index,
                    batch_size,
                    INGESTION_STATS.clone(),
                );
                es_ingest(client, consumer).await.unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    });
}

fn stats_watcher() {
    let interval = std::time::Duration::from_secs(2);
    loop {
        let flushed_count = INGESTION_STATS.num_flushed.load(SeqCst);
        let indexed_count = INDEXED_COUNT.load(SeqCst);
        let fraction_indexed = indexed_count as f64 / flushed_count as f64;
        println!("flushed count: {flushed_count}, indexed count: {indexed_count}, fraction indexed: {fraction_indexed}");
        thread::sleep(interval);
    }
}

fn create_es_index(elastic_builder: ESClientBuilder) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let client = ESBatchedIndexClient::<prep_data::ESSample>::new(
            elastic_builder.build().unwrap(),
            INDEX_NAME.clone(),
            ARGS.es_ingest_batch_size,
            INGESTION_STATS.clone(),
        );

        let mut schema = HashMap::new();
        schema.insert("series_id".into(), ESFieldType::UnsignedLong);
        schema.insert("timestamp".into(), ESFieldType::UnsignedLong);

        client
            .create_index(elastic::CreateIndexArgs {
                num_shards: ARGS.es_num_shards,
                num_replicas: ARGS.es_num_replicas,
                schema: Some(schema),
            })
            .await
            .unwrap();

        println!("ES Index {} created", INDEX_NAME.as_str());
    });
}

fn main() {
    let es_client_config = ESClientBuilder::default()
        .username_optional(ARGS.es_username.clone())
        .password_optional(ARGS.es_password.clone())
        .endpoint(ARGS.es_endpoint.clone());

    create_es_index(es_client_config.clone());

    let (consume_tx, consume_rx) = bounded(10);
    let consumer = thread::spawn(move || {
        kafka_es_consumer(&ARGS.kafka_topic, &ARGS.kafka_bootstraps, consume_tx)
    });
    let es_client_builder = es_client_config.clone();
    let ingestor = thread::spawn(move || {
        es_ingestor(
            es_client_builder,
            &INDEX_NAME,
            ARGS.es_ingest_batch_size,
            ARGS.es_num_writers,
            consume_rx,
        )
    });

    let es_client_builder = es_client_config.clone();
    thread::spawn(move || es_data_age_watcher(es_client_builder, &INDEX_NAME));
    let es_client_builder = es_client_config.clone();
    thread::spawn(move || es_docs_count_watcher(es_client_builder, &INDEX_NAME));
    thread::spawn(stats_watcher);

    consumer.join().unwrap();
    ingestor.join().unwrap();
}
