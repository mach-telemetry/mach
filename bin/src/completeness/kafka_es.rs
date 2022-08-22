use crate::completeness::{
    kafka::{decompress_kafka_msg, kafka_writer},
    Writer,
};
use crate::elastic::{
    CreateIndexArgs, ESBatchedIndexClient, ESClientBuilder, ESIndexQuerier, IngestResponse,
    IngestStats,
};
use crate::prep_data::ESSample;
use crate::{kafka_utils::make_topic, utils::timestamp_now_micros};
use crossbeam_channel::{bounded, Receiver, Sender};
//use elasticsearch::http::request::JsonBody;
use kafka::{client::KafkaClient, consumer::Consumer};
use lazy_static::lazy_static;
use mach::{id::SeriesId};
use std::sync::{atomic::AtomicUsize, Arc, Barrier};
use std::thread;
use std::time::Duration;

lazy_static! {
    static ref ES_INDEX_NAME: String = format!("test-data-{}", timestamp_now_micros());
    static ref NUM_WRITTEN: AtomicUsize = AtomicUsize::new(0);
    static ref INGESTION_STATS: Arc<IngestStats> = Arc::new(IngestStats::default());
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
        client
            .create_index(CreateIndexArgs {
                num_shards: 20,
                num_replicas: 1,
            })
            .await
            .expect("could not create index");

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

async fn es_watch_docs_count(client: ESIndexQuerier, index_name: &String) {
    let one_sec = std::time::Duration::from_secs(1);
    let mut last_doc_count = 0;
    loop {
        match client.query_index_doc_count(index_name).await {
            Ok(resp) => match resp {
                None => println!("index not ready yet"),
                Some(doc_count) => {
                    let delta = doc_count - last_doc_count;
                    println!("indexed doc count: {doc_count}, index docs/sec: {}", delta);
                    last_doc_count = doc_count;
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

fn es_num_docs_flushed_watcher() {
    let one_sec = std::time::Duration::from_secs(1);
    let mut last_val = NUM_WRITTEN.load(std::sync::atomic::Ordering::SeqCst);
    loop {
        let curr_num_written = NUM_WRITTEN.load(std::sync::atomic::Ordering::SeqCst);
        let delta = curr_num_written - last_val;
        println!("num. samples flushed per sec: {}", delta);
        last_val = curr_num_written;
        std::thread::sleep(one_sec);
    }
}

pub fn init_kafka_es(
    kafka_bootstraps: &'static str,
    kafka_topic: &'static str,
    num_writers: usize,
    es_client_builder: ESClientBuilder,
    es_ingest_batch_size: usize,
    es_num_writers: usize,
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
    thread::spawn(move || kafka_es_consumer(kafka_topic, &kafka_bootstraps, consume_tx));
    let es_builder_clone = es_client_builder.clone();
    thread::spawn(move || {
        es_ingestor(
            es_builder_clone,
            &ES_INDEX_NAME,
            es_ingest_batch_size,
            es_num_writers,
            consume_rx,
        )
    });
    let es_builder_clone = es_client_builder.clone();
    thread::spawn(move || es_freshness_watcher(es_builder_clone, &ES_INDEX_NAME));
    thread::spawn(move || es_docs_count_watcher(es_client_builder, &ES_INDEX_NAME));
    thread::spawn(es_num_docs_flushed_watcher);

    Writer {
        sender: tx,
        barrier,
    }
}
