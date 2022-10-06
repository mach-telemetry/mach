#[allow(dead_code)]
mod batching;
#[allow(dead_code)]
mod constants;
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
use constants::PARAMETERS;
use crossbeam_channel::{bounded, Receiver, Sender};
use elastic::{
    ESBatchedIndexClient, ESClientBuilder, ESFieldType, ESIndexQuerier, IngestResponse, IngestStats,
};
use kafka::{client::KafkaClient, consumer::Consumer};
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use utils::timestamp_now_micros;

lazy_static! {
    static ref INGESTION_STATS: Arc<IngestStats> = Arc::new(IngestStats::default());
    static ref INDEXED_COUNT: AtomicU64 = AtomicU64::new(0);
    static ref TOTAL_SAMPLES: AtomicU64 = AtomicU64::new(0);
    static ref WRITTEN_SAMPLES: AtomicU64 = AtomicU64::new(0);
    static ref DECOMPRESSION_TIME_MS: AtomicU64 = AtomicU64::new(0);
    static ref QUEUE_LEN: AtomicUsize = AtomicUsize::new(0);
    static ref SERIALIZE_TIME_MICROS: AtomicUsize = AtomicUsize::new(0);
}

macro_rules! await_on {
    ($async_body:expr) => {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            $async_body.await;
        });
    };
}

type EsWriterInput = batching::BytesBatch;

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
        partition, PARAMETERS.kafka_topic
    );

    kafka_consumer
}

async fn write_to_es(mut es_writer: ESBatchedIndexClient<Vec<u8>>, rx: Receiver<EsWriterInput>) {
    let mut serialize_time = 0;
    let mut decomp_time = 0;

    while let Ok(batch) = rx.recv() {
        let t = Instant::now();
        let entries = batch.entries();
        decomp_time += t.elapsed().as_micros();
        if decomp_time >= 1_000_000 {
            DECOMPRESSION_TIME_MS.fetch_add(decomp_time.try_into().unwrap(), SeqCst);
            decomp_time = 0;
        }

        for entry in entries.iter() {
            let t = Instant::now();
            let bytes = ESSampleRef::from(entry).into();
            serialize_time += t.elapsed().as_micros();

            if serialize_time > 1_000_000 {
                SERIALIZE_TIME_MICROS.fetch_add(serialize_time.try_into().unwrap(), SeqCst);
                serialize_time = 0;
            }

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

    let r = es_writer.flush().await.unwrap();
    assert!(r.status_code().is_success());
}

#[allow(dead_code)]
fn consume_or_drop_from_kafka(mut consumer: Consumer, tx: Sender<EsWriterInput>) {
    loop {
        for mset in consumer.poll().unwrap().iter() {
            for m in mset.messages() {
                let batch = BytesBatch::new(m.value.into());
                let count = batch.count() as u64;
                TOTAL_SAMPLES.fetch_add(count, SeqCst);
                match tx.try_send(batch) {
                    Ok(_) => {
                        WRITTEN_SAMPLES.fetch_add(count, SeqCst);
                    }
                    Err(_) => {}
                }
            }
        }
    }
}

#[allow(dead_code)]
fn blocking_consume_from_kafka(mut consumer: Consumer, tx: Sender<EsWriterInput>) {
    loop {
        for mset in consumer.poll().unwrap().iter() {
            for m in mset.messages() {
                let batch = BytesBatch::new(m.value.into());
                let count = batch.count() as u64;
                TOTAL_SAMPLES.fetch_add(count, SeqCst);
                WRITTEN_SAMPLES.fetch_add(count, SeqCst);
                tx.send(batch).unwrap();
            }
        }
    }
}

fn kafka_consumer(partition: i32, tx: Sender<EsWriterInput>) {
    let kafka_consumer = make_kafka_consumer(
        PARAMETERS.kafka_bootstraps.as_str(),
        PARAMETERS.kafka_topic.as_str(),
        &[partition.try_into().unwrap()],
    );

    if PARAMETERS.unbounded_queue {
        blocking_consume_from_kafka(kafka_consumer, tx);
    } else {
        consume_or_drop_from_kafka(kafka_consumer, tx);
    }
}

fn es_writer(es_conf: ESClientBuilder, rx: Receiver<EsWriterInput>) {
    let es_writer = ESBatchedIndexClient::new(
        es_conf.build().unwrap(),
        PARAMETERS.es_index_name.clone(),
        PARAMETERS.es_batch_bytes,
        INGESTION_STATS.clone(),
    );

    await_on!(write_to_es(es_writer, rx));
}

fn stats_watcher() {
    let mut prev_bytes_written = 0;
    let mut prev_samples_flushed = 0;
    let interval = Duration::from_secs(PARAMETERS.print_interval_seconds);

    loop {
        let flushed_count = INGESTION_STATS.num_flushed.load(SeqCst);
        let indexed_count = INDEXED_COUNT.load(SeqCst);
        let fraction_indexed = indexed_count as f64 / flushed_count as f64;

        let flushed_count_delta = flushed_count - prev_samples_flushed;
        prev_samples_flushed = flushed_count;
        let samples_per_sec = flushed_count_delta as f64 / interval.as_secs_f64();

        let bytes_written = INGESTION_STATS.bytes_flushed.load(SeqCst);
        let flushed_delta = bytes_written - prev_bytes_written;
        let bytes_per_sec = flushed_delta as f64 / interval.as_secs_f64();
        prev_bytes_written = bytes_written;

        let num_flushes_initiated = INGESTION_STATS.num_flush_reqs_initiated.load(SeqCst);
        let num_flushes_completed = INGESTION_STATS.num_flush_reqs_completed.load(SeqCst);
        let num_flushes_pending = num_flushes_initiated - num_flushes_completed;
        let num_flush_retries = INGESTION_STATS.num_flush_retries.load(SeqCst);
        let total_flush_ms = INGESTION_STATS.flush_dur_millis.load(SeqCst);
        let avg_flush_dur_ms = total_flush_ms as f64 / num_flushes_completed as f64;
        let cumulative_flush_avg = total_flush_ms as f64 / num_flushes_initiated as f64;

        let total_serialize_time_micros = SERIALIZE_TIME_MICROS.load(SeqCst);
        let total_serialize_time_ms = total_serialize_time_micros / 1_000;
        let per_batch_serialize_time =
            total_serialize_time_ms as f64 / num_flushes_initiated as f64;

        let total_decomp_ms = DECOMPRESSION_TIME_MS.load(SeqCst);
        let per_batch_decomp_ms = total_decomp_ms as f64 / num_flushes_initiated as f64;

        let consumed_samples = TOTAL_SAMPLES.load(SeqCst);
        let written_samples = WRITTEN_SAMPLES.load(SeqCst);
        let completeness = written_samples as f64 / consumed_samples as f64;

        println!(
            "samples per sec: {samples_per_sec}, bytes per sec: {bytes_per_sec}, \
            flushed count: {flushed_count}, \
            indexed count: {indexed_count}, fraction indexed: {fraction_indexed}, \
                 flushes (init: {num_flushes_initiated}, completed: {num_flushes_completed}, \
                          pending: {num_flushes_pending}, retries: {num_flush_retries}, \
                          avg dur {avg_flush_dur_ms} ms), \
                 per batch stats (decomp ms: {per_batch_decomp_ms}, \
                                  serialize: {per_batch_serialize_time}, \
                                  flush: {cumulative_flush_avg}) \
                 completeness: {completeness}"
        );

        thread::sleep(interval);
    }
}

fn create_es_index(elastic_builder: ESClientBuilder) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let client = ESBatchedIndexClient::<prep_data::ESSample>::new(
            elastic_builder.build().unwrap(),
            PARAMETERS.es_index_name.clone(),
            PARAMETERS.es_batch_bytes,
            INGESTION_STATS.clone(),
        );

        let mut schema = HashMap::new();
        schema.insert("series_id".into(), ESFieldType::Keyword);

        let r = client
            .create_index(elastic::CreateIndexArgs {
                num_shards: PARAMETERS.es_num_shards,
                num_replicas: PARAMETERS.es_num_replicas,
                schema: Some(schema),
            })
            .await
            .unwrap();

        assert!(r.status_code().is_success());

        println!(
            "ES Index {} created, resp: {:?}",
            PARAMETERS.es_index_name.as_str(),
            r
        );
    });
}

fn main() {
    let es_client_config = ESClientBuilder::default()
        .username_optional(PARAMETERS.es_username.clone())
        .password_optional(PARAMETERS.es_password.clone())
        .endpoint(PARAMETERS.es_endpoint.clone());

    create_es_index(es_client_config.clone());

    let mut consumers = Vec::new();
    let mut writers = Vec::new();
    for partition in 0..PARAMETERS.kafka_partitions {
        let es_conf = es_client_config.clone();
        let (tx, rx) = bounded(100);
        consumers.push(thread::spawn(move || kafka_consumer(partition as i32, tx)));
        writers.push(thread::spawn(move || es_writer(es_conf, rx)));
    }

    let es_client_builder = es_client_config.clone();
    thread::spawn(move || es_data_age_watcher(es_client_builder, &PARAMETERS.es_index_name));
    let es_client_builder = es_client_config.clone();
    thread::spawn(move || es_docs_count_watcher(es_client_builder, &PARAMETERS.es_index_name));
    thread::spawn(stats_watcher);

    for c in consumers {
        c.join().unwrap();
    }
    for w in writers {
        w.join().unwrap();
    }
}
