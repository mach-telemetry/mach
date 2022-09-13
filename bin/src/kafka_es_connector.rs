#[allow(dead_code)]
mod completeness;
#[allow(dead_code)]
mod elastic;
#[allow(dead_code)]
mod kafka_utils;
#[allow(dead_code)]
mod prep_data;
#[allow(dead_code)]
mod utils;

use crate::completeness::{Decompress, SingleSourceBatch};
use crate::prep_data::ESSample;
use clap::*;
use crossbeam_channel::{bounded, Receiver, Sender};
use elastic::{
    ESBatchedIndexClient, ESClientBuilder, ESFieldType, ESIndexQuerier, IngestResponse, IngestStats,
};
use kafka::consumer::{Message, MessageSets};
use kafka::{client::KafkaClient, consumer::Consumer};
use lazy_static::lazy_static;
use mach::id::SeriesId;
use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::Instant;
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

    #[clap(short, long, default_value_t = 5000)]
    es_ingest_batch_size: usize,

    #[clap(short, long, default_value_t = 40)]
    es_num_writers: usize,

    #[clap(short, long, default_value_t = 3)]
    es_num_shards: usize,

    #[clap(short, long, default_value_t = 0)]
    es_num_replicas: usize,
}

type ESIngestorInput = KafkaMessage;

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

struct KafkaMessage {
    message_ptr: u64,
    #[allow(dead_code)]
    _owning_message_sets: Arc<MessageSets>,
}

impl KafkaMessage {
    fn new(message: &Message<'_>, owning_message_sets: Arc<MessageSets>) -> Self {
        let msg: *const Message<'_> = message;

        Self {
            message_ptr: msg as u64,
            _owning_message_sets: owning_message_sets,
        }
    }

    fn message(&self) -> &Message<'_> {
        unsafe { &*(self.message_ptr as *const Message<'_>) }
    }
}

fn consume_from_kakfka(
    topic: &str,
    bootstraps: &str,
    consumer_group: &str,
    partition: i32,
    senders: Vec<Arc<Sender<ESIngestorInput>>>,
) {
    let mut kafka_client = KafkaClient::new(bootstraps.split(',').map(String::from).collect());
    kafka_client.load_metadata_all().unwrap();
    let mut kafka_consumer = Consumer::from_client(kafka_client)
        .with_topic_partitions(topic.to_owned(), &[partition])
        .with_fetch_max_bytes_per_partition(10_000_000)
        .with_group(consumer_group.to_owned())
        .create()
        .unwrap();

    println!(
        "Kafka consumer created, begin consuming from topic {}",
        topic
    );

    loop {
        let message_sets = Arc::new(
            kafka_consumer
                .poll()
                .expect("failed to get the next set of messsages from kafka"),
        );

        for ms in message_sets.iter() {
            for m in ms.messages().iter() {
                let source_id = SingleSourceBatch::<SeriesId>::peek_source_id(m.value);
                let picked_writer = &senders[source_id % senders.len()];
                picked_writer
                    .send(KafkaMessage::new(m, message_sets.clone()))
                    .unwrap();
                QUEUE_LEN.fetch_add(1, SeqCst);
            }
            kafka_consumer.consume_messageset(ms);
        }
    }
}

fn kafka_es_consumer(
    topic: &'static str,
    bootstraps: &'static str,
    senders: Vec<Sender<ESIngestorInput>>,
) {
    let num_partitions = 3;
    let consumer_group_name = "cgroup";
    let senders: Vec<_> = senders.into_iter().map(|s| Arc::new(s)).collect();

    let mut handles = Vec::new();
    for partition in 0..num_partitions {
        let senders = senders.clone();
        handles.push(thread::spawn(move || {
            consume_from_kakfka(topic, bootstraps, consumer_group_name, partition, senders)
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

async fn es_ingest(
    mut client: ESBatchedIndexClient<ESSample>,
    consumer: Receiver<ESIngestorInput>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut buffer = vec![0u8; 500_000_000];
    while let Ok(payload) = consumer.recv() {
        QUEUE_LEN.fetch_sub(1, SeqCst);

        let start = Instant::now();
        let batch = SingleSourceBatch::<SeriesId>::decompress(
            payload.message().value,
            buffer.as_mut_slice(),
        );
        let decomp_dur = start.elapsed();
        DECOMPRESSION_TIME_MS.fetch_add(decomp_dur.as_millis().try_into().unwrap(), SeqCst);
        NUM_MSGS_DECOMPRESSED.fetch_add(1, SeqCst);

        let source_id = SeriesId(batch.source_id.try_into().unwrap());

        let samples: Vec<ESSample> = batch
            .data
            .into_iter()
            .map(|s| ESSample::new(source_id, s.0, s.1))
            .collect();

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
    es_batch_size: usize,
    num_ingestors: usize,
    consumers: Vec<Receiver<ESIngestorInput>>,
) {
    assert!(num_ingestors == consumers.len());

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let handles: Vec<JoinHandle<()>> = consumers
            .into_iter()
            .map(|consumer| {
                let c = consumer.clone();
                let client = ESBatchedIndexClient::new(
                    es_client_builder.clone().build().unwrap(),
                    index_name.clone(),
                    es_batch_size,
                    INGESTION_STATS.clone(),
                );
                tokio::spawn(async move {
                    es_ingest(client, c).await.unwrap();
                })
            })
            .collect();

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

        println!("flushed count: {flushed_count}, indexed count: {indexed_count}, fraction indexed: {fraction_indexed}, queue len: {queue_len}, flushes (init: {num_flushes_initiated}, completed: {num_flushes_completed}, pending: {num_flushes_pending}, retries: {}, avg dur {avg_flush_dur_ms} ms), avg decmp ms: {avg_decomp_ms}");
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

    let mut producers = Vec::new();
    let mut consumers = Vec::new();
    for _ in 0..ARGS.es_num_writers {
        let (tx, rx) = bounded(1000);
        producers.push(tx);
        consumers.push(rx);
    }

    let consumer = thread::spawn(move || {
        kafka_es_consumer(&ARGS.kafka_topic, &ARGS.kafka_bootstraps, producers)
    });
    let es_client_builder = es_client_config.clone();
    let ingestor = thread::spawn(move || {
        es_ingestor(
            es_client_builder,
            &INDEX_NAME,
            ARGS.es_ingest_batch_size,
            ARGS.es_num_writers,
            consumers,
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
