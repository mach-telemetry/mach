#![deny(unused_must_use)]

mod sample;

use futures::executor;
use lazy_static::*;
use mach::{
    compression::{CompressFn, Compression},
    durable_queue::{KafkaConfig, FileConfig, QueueConfig},
//    id::{SeriesId, WriterId},
//    reader::{ReadResponse, ReadServer},
    sample::Type,
    series::{SeriesConfig, Types},
    tags::Tags,
    tsdb::Mach,
    utils::random_id,
    writer::WriterConfig,
};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
    consumer::{
        stream_consumer::StreamConsumer, CommitMode, Consumer,
        DefaultConsumerContext,
    },
    producer::{FutureProducer, FutureRecord},
    Message,
};
use serde_json;
use std::fs::*;
use std::io::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering::SeqCst},
    mpsc::{channel, Receiver, Sender},
    Arc, Barrier,
};
use std::thread;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use zstd::stream::{decode_all, encode_all};

const ENDPOINT: &str = &"kafka"; // "mach", "kafka"
const BATCH_SIZE: usize = 8192 * 2;
const DATA_PATH: &str = &"/home/fsolleza/data/mach/demo_data";
const KAFKA_PARTITIONS: i32 = 10;
const KAFKA_REPLICATION: i32 = 3;
const KAFKA_ACKS: &str = &"all";
const KAFKA_BOOTSTRAPS: &str = &"localhost:9093,localhost:9094,localhost:9095";
const MACH_FILE_OUT: &str = &"/home/fsolleza/data/mach/tmp";
const MACH_STORAGE: &str = "kafka"; // "file", "kafka"

lazy_static! {
    static ref KAFKA_TOPIC: String = random_id();
    static ref COUNTER_BARRIER: Arc<Barrier> = Arc::new(Barrier::new(4));
    static ref SAMPLE_COUNTER: Arc<AtomicUsize> = {
        let counter = Arc::new(AtomicUsize::new(0));
        let sc = counter.clone();
        thread::spawn(move || {
            let barrier = COUNTER_BARRIER.clone();
            barrier.wait();
            loop {
                thread::sleep(Duration::from_secs(1));
                let cur = sc.load(SeqCst);
                println!("Unprocessed samples {}", cur);
            }
        });
        counter
    };
    static ref PRODUCER_COUNTER: Arc<AtomicUsize> = {
        let counter = Arc::new(AtomicUsize::new(0));
        let pc = counter.clone();
        thread::spawn(move || {
            let barrier = COUNTER_BARRIER.clone();
            barrier.wait();
            let mut last = pc.load(SeqCst);
            loop {
                thread::sleep(Duration::from_secs(1));
                let cur = pc.load(SeqCst);
                println!("Producer Rate {} / sec", cur - last);
                last = cur;
            }
        });
        counter
    };
    static ref CONSUMER_COUNTER: Arc<AtomicUsize> = {
        let counter = Arc::new(AtomicUsize::new(0));
        let pc = counter.clone();
        thread::spawn(move || {
            let barrier = COUNTER_BARRIER.clone();
            barrier.wait();
            let mut last = pc.load(SeqCst);
            loop {
                thread::sleep(Duration::from_secs(1));
                let cur = pc.load(SeqCst);
                println!("Consumer Rate {} / sec", cur - last);
                last = cur;
            }
        });
        counter
    };
    static ref FILE_CONF: QueueConfig = FileConfig {
        dir: MACH_FILE_OUT.into(),
        file: random_id(),
    }
    .config();
    static ref KAFKA_CONF: QueueConfig = KafkaConfig {
        bootstrap: KAFKA_BOOTSTRAPS.into(),
        topic: random_id(),
    }
    .config();
}

fn kafka_producer(rx: Receiver<sample::Sample>) {
    let sc = SAMPLE_COUNTER.clone();
    let pc = PRODUCER_COUNTER.clone();
    let producer_topic = KAFKA_TOPIC.clone();

    println!("Making producer");
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", KAFKA_BOOTSTRAPS)
        .set("acks", &*KAFKA_ACKS)
        .set("message.max.bytes", "1000000000")
        .set("linger.ms", "0")
        .set("message.copy.max.bytes", "5000000")
        .set("message.timeout.ms", "3000")
        .create()
        .unwrap();

    let mut buf = Vec::new();
    println!("Begin waiting for samples");
    while let Ok(sample) = rx.recv() {
        sc.fetch_sub(1, SeqCst);
        buf.push(sample);
        if buf.len() == BATCH_SIZE {
            let encoded = bincode::serialize(&buf).unwrap();
            let bytes = encode_all(encoded.as_slice(), 0).unwrap();
            let to_send: FutureRecord<str, [u8]> =
                FutureRecord::to(&producer_topic).payload(&bytes);
            match executor::block_on(producer.send(to_send, Duration::from_secs(3))) {
                Ok(_) => {
                    pc.fetch_add(BATCH_SIZE, SeqCst);
                }
                Err(_) => println!("Produce error"),
            }
            buf.clear();
        }
    }
}

fn detect_config(tags: &Tags, sample: &sample::Sample) -> SeriesConfig {
    let mut types = Vec::new();
    let mut compression = Vec::new();
    for v in sample.values.iter() {
        match v {
            sample::Type::F64(_) => {
                types.push(Types::F64);
                compression.push(CompressFn::Decimal(3));
            }
            sample::Type::Str(_) => {
                types.push(Types::Bytes);
                compression.push(CompressFn::BytesLZ4);
            }
        }
    }
    let compression = Compression::from(compression);
    let seg_count = 3;
    let nvars = types.len();

    SeriesConfig {
        tags: tags.clone(),
        types,
        compression,
        seg_count,
        nvars,
    }
}

fn mach_writer(rx: Receiver<sample::Sample>) {
    let sc = SAMPLE_COUNTER.clone();
    let pc = PRODUCER_COUNTER.clone();

    // Setup mach and writer
    let mut mach = Mach::new();
    let queue_config = match MACH_STORAGE {
        "file" => FILE_CONF.clone(),
        "kafka" => KAFKA_CONF.clone(),
        _ => panic!("unsupported storage"),
    };

    let writer_config = WriterConfig {
        queue_config,
        active_block_flush_sz: 1_000_000,
    };
    let mut writer = mach.add_writer(writer_config).unwrap();

    let mut set = HashMap::new();
    let mut values = Vec::new();
    while let Ok(sample) = rx.recv() {
        sc.fetch_sub(1, SeqCst);
        let tags = Tags::from(sample.tags.clone());
        let series_id = tags.id();
        let series_ref = *set.entry(series_id).or_insert_with(|| {
            let _ = mach.add_series(detect_config(&tags, &sample)).unwrap();
            let series_ref = writer.get_reference(series_id);
            series_ref
        });

        values.clear();
        for v in &sample.values {
            values.push(Type::from(v));
        }

        loop {
            if writer
                .push(series_ref, sample.timestamp, values.as_slice())
                .is_ok()
            {
                pc.fetch_add(1, SeqCst);
                break;
            }
        }
    }
}

fn mach_consumer() {
    let _consumer_counter = CONSUMER_COUNTER.clone();
}

fn generator(to_producer: Sender<sample::Sample>) {
    let cb = COUNTER_BARRIER.clone();
    let sc = SAMPLE_COUNTER.clone();

    println!("Loading data");
    let reader = BufReader::new(File::open(DATA_PATH).unwrap());

    let data: Vec<sample::Sample> = reader
        .lines()
        .map(|x| {
            let sample: sample::Sample = serde_json::from_str(&x.unwrap()).unwrap();
            sample
        })
        .collect();
    println!("Done loading data");

    // Write data to single writer
    println!("WAITING FOR CONSUMER");
    cb.wait();
    println!("Writing data");

    loop {
        for sample in data.iter() {
            let mut sample = sample.clone();
            sample.timestamp = millis_now();
            if to_producer.send(sample).is_err() {
                panic!("Failed to send");
            };
            sc.fetch_add(1, SeqCst);
        }
    }
}

fn make_topic() {
    let topic = KAFKA_TOPIC.clone();
    let client: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", KAFKA_BOOTSTRAPS)
        .create()
        .unwrap();
    let admin_opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(3)));
    let topics = &[NewTopic {
        name: topic.as_str(),
        num_partitions: KAFKA_PARTITIONS,
        replication: TopicReplication::Fixed(KAFKA_REPLICATION),
        config: Vec::new(),
    }];
    executor::block_on(client.create_topics(topics, &admin_opts)).unwrap();
}

fn kafka_main() {

    make_topic();

    // init the consumer
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.spawn(kafka_consumer());

    // init the generator and producer
    let (tx, rx) = channel::<sample::Sample>();
    let mut handles = Vec::new();
    handles.push(thread::spawn(move || kafka_producer(rx)));
    handles.push(thread::spawn(move || generator(tx)));

    for h in handles {
        h.join().unwrap();
    }
}

fn mach_main() {
    // init the generator and producer
    let (tx, rx) = channel::<sample::Sample>();
    let mut handles = Vec::new();
    handles.push(thread::spawn(move || mach_consumer()));
    handles.push(thread::spawn(move || mach_writer(rx)));
    handles.push(thread::spawn(move || generator(tx)));
    for h in handles {
        h.join().unwrap();
    }
}

fn main() {
    match ENDPOINT {
        "mach" => mach_main(),
        "kafka" => kafka_main(),
        _ => panic!("Invalid endpoint")
    }
}

fn millis_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}

async fn kafka_consumer() {
    let consumer_counter = CONSUMER_COUNTER.clone();
    let consumer: StreamConsumer<DefaultConsumerContext> = ClientConfig::new()
        .set("bootstrap.servers", KAFKA_BOOTSTRAPS)
        .set("group.id", random_id())
        .create()
        .unwrap();
    consumer
        .subscribe(&[&*KAFKA_TOPIC])
        .expect("Can't subscribe to specified topics");

    println!("Reading data");

    loop {
        match consumer.recv().await {
            Err(e) => println!("Kafka error: {}", e),
            Ok(m) => {
                match m.payload_view::<[u8]>() {
                    None => {}
                    Some(Ok(s)) => {
                        let d = decode_all(s).unwrap();
                        if let Ok(x) = bincode::deserialize::<Vec<sample::Sample>>(d.as_slice()) {
                            consumer_counter.fetch_add(BATCH_SIZE, SeqCst);
                            let last_timestamp = x.last().unwrap().timestamp;
                            let now: u64 = millis_now();
                            let data_latency = Duration::from_millis(now - last_timestamp);
                            println!("GAP: {:?}", data_latency);
                        } else {
                            println!("SOMETHING WENT WRONG HERE")
                        }
                    }
                    Some(Err(_)) => {
                        println!("Error while deserializing message payload");
                    }
                };
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        }
    }
}
