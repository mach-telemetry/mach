#[allow(dead_code)]
mod batching;
#[allow(dead_code)]
mod elastic;
#[allow(dead_code)]
mod kafka_utils;
#[allow(dead_code)]
mod prep_data;
#[allow(dead_code)]
mod utils;

use crate::batching::BytesBatch;
use crate::prep_data::ESSampleRef;
use clap::*;
use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};
use elastic::{
    ESBatchedIndexClient, ESClientBuilder, ESFieldType, ESIndexQuerier, IngestResponse, IngestStats,
};
use kafka::consumer::MessageSets;
use kafka::{client::KafkaClient, consumer::Consumer};
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::convert::TryInto;
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
    static ref DECOMPRESSION_TIME_MS: AtomicU64 = AtomicU64::new(0);
    static ref NUM_MSGS_DECOMPRESSED: AtomicU64 = AtomicU64::new(0);
    static ref QUEUE_LEN: AtomicUsize = AtomicUsize::new(0);
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

    #[clap(short, long, default_value_t = 1_000_000)]
    batch_bytes: usize,

    #[clap(short, long, default_value_t = 10)]
    num_threads: usize,

    #[clap(short, long, default_value_t = 10)]
    es_num_shards: usize,

    #[clap(short, long, default_value_t = 0)]
    es_num_replicas: usize,
}

macro_rules! await_on {
    ($async_body:expr) => {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            $async_body.await;
        });
    };
}

type EsWriterInput = Arc<MessageSets>;

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
    await_on!(es_watch_data_age(querier, index_name));
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
    await_on!(es_watch_docs_count(querier, index_name));
}

fn make_kafka_consumer(bootstraps: &str, topic: &str, partition: &[i32]) -> Consumer {
    let mut kafka_client = KafkaClient::new(bootstraps.split(',').map(String::from).collect());
    kafka_client.load_metadata_all().unwrap();
    let kafka_consumer = Consumer::from_client(kafka_client)
        .with_topic_partitions(topic.to_owned(), partition)
        .with_fetch_max_bytes_per_partition(10_000_000)
        .create()
        .unwrap();

    println!(
        "Kafka consumer created, begin consuming {:?} from topic {}",
        partition, ARGS.kafka_topic
    );

    kafka_consumer
}

async fn write_to_es(mut es_writer: ESBatchedIndexClient<Vec<u8>>, rx: Receiver<EsWriterInput>) {
    while let Ok(msets) = rx.recv() {
        for mset in msets.iter() {
            for m in mset.messages() {
                //let mut v = Vec::new();
                //v.extend_from_slice(m.value);

                //let bytes = v.as_slice().into();
                let entries = BytesBatch::new(m.value.into()).entries();

                for entry in entries.iter() {
                    let bytes = ESSampleRef::from(entry).into();
                    match es_writer.ingest(bytes).await {
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
        }
    }

    let r = es_writer.flush().await.unwrap();
    assert!(r.status_code().is_success());
}

#[allow(dead_code)]
fn consume_or_drop_from_kafka(mut consumer: Consumer, tx: Sender<EsWriterInput>) {
    loop {
        let msets = Arc::new(consumer.poll().unwrap());
        if let Err(e) = tx.try_send(msets) {
            match e {
                TrySendError::Full(_) => continue,
                TrySendError::Disconnected(_) => break,
            }
        }
    }
}

#[allow(dead_code)]
fn blocking_consume_from_kafka(mut consumer: Consumer, tx: Sender<EsWriterInput>) {
    'outer: loop {
        let msets = Arc::new(consumer.poll().unwrap());
        'send: loop {
            let msets = msets.clone();
            match tx.try_send(msets) {
                Ok(_) => break 'send,
                Err(e) => match e {
                    TrySendError::Full(_) => continue,
                    TrySendError::Disconnected(_) => break 'outer,
                },
            }
        }
    }
}

fn kafka_consumer(partition: i32, tx: Sender<EsWriterInput>) {
    let kafka_consumer = make_kafka_consumer(
        ARGS.kafka_bootstraps.as_str(),
        ARGS.kafka_topic.as_str(),
        &[partition.try_into().unwrap()],
    );

    consume_or_drop_from_kafka(kafka_consumer, tx);
}

fn es_writer(es_conf: ESClientBuilder, rx: Receiver<EsWriterInput>) {
    let es_writer = ESBatchedIndexClient::new(
        es_conf.build().unwrap(),
        INDEX_NAME.clone(),
        ARGS.batch_bytes,
        INGESTION_STATS.clone(),
    );

    await_on!(write_to_es(es_writer, rx));
}

fn stats_watcher() {
    let interval = std::time::Duration::from_secs(2);
    loop {
        let flushed_count = INGESTION_STATS.num_flushed.load(SeqCst);
        let indexed_count = INDEXED_COUNT.load(SeqCst);
        let fraction_indexed = indexed_count as f64 / flushed_count as f64;
        let queue_len = QUEUE_LEN.load(SeqCst);

        let num_flushes_initiated = INGESTION_STATS.num_flush_reqs_initiated.load(SeqCst);
        let num_flushes_completed = INGESTION_STATS.num_flush_reqs_completed.load(SeqCst);
        let num_flushes_pending = num_flushes_initiated - num_flushes_completed;
        let num_flush_retries = INGESTION_STATS.num_flush_retries.load(SeqCst);
        let total_flush_ms = INGESTION_STATS.flush_dur_millis.load(SeqCst);
        let avg_flush_dur_ms = total_flush_ms as f64 / num_flushes_completed as f64;

        let total_decomp_ms = DECOMPRESSION_TIME_MS.load(SeqCst);
        let num_decomps = NUM_MSGS_DECOMPRESSED.load(SeqCst);
        let avg_decomp_ms = total_decomp_ms as f64 / num_decomps as f64;

        println!(
            "flushed count: {flushed_count}, indexed count: {indexed_count}, \
                 fraction indexed: {fraction_indexed}, queue len: {queue_len}, \
                 flushes (init: {num_flushes_initiated}, completed: {num_flushes_completed}, \
                          pending: {num_flushes_pending}, retries: {num_flush_retries}, \
                          avg dur {avg_flush_dur_ms} ms), \
                 avg decmp ms: {avg_decomp_ms}"
        );

        thread::sleep(interval);
    }
}

fn create_es_index(elastic_builder: ESClientBuilder) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let client = ESBatchedIndexClient::<prep_data::ESSample>::new(
            elastic_builder.build().unwrap(),
            INDEX_NAME.clone(),
            ARGS.batch_bytes,
            INGESTION_STATS.clone(),
        );

        let mut schema = HashMap::new();
        schema.insert("series_id".into(), ESFieldType::UnsignedLong);
        schema.insert("timestamp".into(), ESFieldType::UnsignedLong);

        let r = client
            .create_index(elastic::CreateIndexArgs {
                num_shards: ARGS.es_num_shards,
                num_replicas: ARGS.es_num_replicas,
                schema: Some(schema),
            })
            .await
            .unwrap();

        assert!(r.status_code().is_success());

        println!("ES Index {} created", INDEX_NAME.as_str());
    });
}

fn main() {
    let es_client_config = ESClientBuilder::default()
        .username_optional(ARGS.es_username.clone())
        .password_optional(ARGS.es_password.clone())
        .endpoint(ARGS.es_endpoint.clone());

    create_es_index(es_client_config.clone());

    let mut consumers = Vec::new();
    let mut writers = Vec::new();
    for partition in 0..ARGS.num_threads {
        let es_conf = es_client_config.clone();
        let (tx, rx) = bounded(1);
        consumers.push(thread::spawn(move || kafka_consumer(partition as i32, tx)));
        writers.push(thread::spawn(move || es_writer(es_conf, rx)));
    }

    let es_client_builder = es_client_config.clone();
    thread::spawn(move || es_data_age_watcher(es_client_builder, &INDEX_NAME));
    let es_client_builder = es_client_config.clone();
    thread::spawn(move || es_docs_count_watcher(es_client_builder, &INDEX_NAME));
    thread::spawn(stats_watcher);

    for c in consumers {
        c.join().unwrap();
    }
    for w in writers {
        w.join().unwrap();
    }
}
