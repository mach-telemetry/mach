#[allow(dead_code)]
mod bytes_server;
#[allow(dead_code)]
mod prep_data;
#[allow(dead_code)]
mod snapshotter;

use clap::*;
use elasticsearch::{http::request::JsonBody, BulkParts, Elasticsearch, IndexParts, SearchParts};
use kafka::{
    client::{FetchOffset, KafkaClient},
    consumer::{Consumer, Message},
};
use lazy_static::lazy_static;
use lzzzz::lz4;
use mach::{
    id::{SeriesId, SeriesRef},
    mem_list::{TOTAL_BYTES_FLUSHED, UNFLUSHED_COUNT},
    sample::SampleType,
    series::Series,
    tsdb::Mach,
    utils::kafka::{make_topic, Producer},
    writer::{Writer as MachWriter, WriterConfig},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc, Barrier, Mutex,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crossbeam::channel::{bounded, Receiver, Sender};

lazy_static! {
    static ref ARGS: Args = Args::parse();
    static ref SAMPLES: Vec<prep_data::Sample> = prep_data::load_samples(ARGS.file_path.as_str());
    static ref SERIES_IDS: Vec<SeriesId> = {
        let mut set = HashSet::new();
        for sample in SAMPLES.iter() {
            set.insert(sample.0);
        }
        let ids = set.drain().collect();
        println!("IDs {:?}", ids);
        ids
    };
    static ref MACH_SAMPLES: Vec<prep_data::RegisteredSample> = {
        let mach = MACH.clone(); // ensure MACH is initialized (prevent deadlock)
        let writer = MACH_WRITER.clone(); // ensure WRITER is initialized (prevent deadlock)
        let mut mach_guard = mach.lock().unwrap();
        let mut writer_guard = writer.lock().unwrap();
        let samples = SAMPLES.as_slice();
        prep_data::mach_register_samples(samples, &mut *mach_guard, &mut *writer_guard)
    };
    static ref MACH: Arc<Mutex<Mach>> = Arc::new(Mutex::new(Mach::new()));
    static ref MACH_WRITER: Arc<Mutex<MachWriter>> = {
        let mach = MACH.clone(); // ensure MACH is initialized (prevent deadlock)
        let writer_config = WriterConfig {
            active_block_flush_sz: 1_000_000,
        };
        let mut guard = mach.lock().unwrap();
        Arc::new(Mutex::new(guard.add_writer(writer_config).unwrap()))
    };
    static ref DECOMPRESS_BUFFER: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(vec![0u8; 1_000_000]));
    static ref COUNTERS: Counters = Counters::new();
    static ref ES_INDEX_NAME: String = format!("test-data-{}", timestamp_now());
}

#[derive(Parser, Debug, Clone)]
struct Args {
    #[clap(short, long, default_value_t = String::from("kafka"))]
    tsdb: String,

    #[clap(short, long, default_value_t = 4)]
    kafka_writers: usize,

    #[clap(short, long, default_value_t = String::from("localhost:9093,localhost:9094,localhost:9095"))]
    kafka_bootstraps: String,

    #[clap(short, long, default_value_t = 3)]
    kafka_partitions: usize,

    #[clap(short, long, default_value_t = 3)]
    kafka_replicas: usize,

    #[clap(short, long, default_value_t = String::from("kafka-completeness-bench"))]
    kafka_topic: String,

    #[clap(short, long, default_value_t = 100000)]
    batch_size: u32,

    #[clap(short, long, default_value_t = String::from("/home/sli/data/train-ticket-data"))]
    file_path: String,

    #[clap(short, long, default_value_t = 5.0)]
    counter_interval_seconds: f64,
}

struct Counters {
    samples_enqueued: Arc<AtomicUsize>,
    samples_dropped: Arc<AtomicUsize>,
    samples_written: Arc<AtomicUsize>,
    unflushed_count: Arc<AtomicUsize>,
    bytes_flushed: Arc<AtomicUsize>,
    raw_data_size: Arc<AtomicUsize>,
    data_age: Arc<AtomicUsize>,
    //last_timestamp: u64,
    start_gate: Arc<Barrier>,
}

impl Counters {
    fn new() -> Self {
        let r = Self {
            samples_enqueued: Arc::new(AtomicUsize::new(0)),
            samples_dropped: Arc::new(AtomicUsize::new(0)),
            samples_written: Arc::new(AtomicUsize::new(0)),
            raw_data_size: Arc::new(AtomicUsize::new(0)),
            data_age: Arc::new(AtomicUsize::new(0)),
            //last_timestamp: micros_from_epoch().try_into().unwrap(),
            unflushed_count: UNFLUSHED_COUNT.clone(),
            bytes_flushed: TOTAL_BYTES_FLUSHED.clone(),
            start_gate: Arc::new(Barrier::new(2)),
        };
        r
    }

    fn start_watcher(&self) {
        self.start_gate.wait();
    }

    fn init_watcher(&self) {
        let b = self.start_gate.clone();
        thread::spawn(move || {
            watcher(b);
        });
    }

    fn init_kafka_consumer(&self) {
        thread::spawn(move || {
            init_kafka_consumer();
        });
    }

    fn init_mach_querier(&self) {
        //let k = self.data_age.clone();
        thread::spawn(move || {
            init_mach_querier();
        });
    }
}

fn watcher(start_gate: Arc<Barrier>) {
    //const look_back: usize = 5;
    let interval = Duration::from_secs_f64(ARGS.counter_interval_seconds);
    //let look_back_f64: f64 = (look_back as u32).try_into().unwrap();
    start_gate.wait();
    //let mut look_back_array: [f64; look_back] = [0.; look_back];
    let mut last_pushed = 0;
    let mut last_dropped = 0;
    //let mut counter = 0;
    loop {
        let pushed = COUNTERS.samples_enqueued.load(SeqCst);
        let dropped = COUNTERS.samples_dropped.load(SeqCst);
        let _written = COUNTERS.samples_written.load(SeqCst);
        let raw_data_size = COUNTERS.raw_data_size.load(SeqCst);

        let pushed_since = pushed - last_pushed;
        let dropped_since = dropped - last_dropped;
        let total_since = pushed_since + dropped_since;
        let completeness: f64 = {
            let pushed: f64 = (pushed_since as u32).try_into().unwrap();
            let total: f64 = (total_since as u32).try_into().unwrap();
            pushed / total
        };
        let rate: f64 = {
            let pushed: f64 = (pushed_since as u32).try_into().unwrap();
            let secs = interval.as_secs_f64();
            pushed / secs
        };

        let data_age = COUNTERS.data_age.load(SeqCst) as u64;
        let delay = Duration::from_micros(data_age);

        last_pushed = pushed;
        last_dropped = dropped;
        //counter += 1;

        let bytes_flushed = COUNTERS.bytes_flushed.load(SeqCst);
        let unflushed_count = COUNTERS.unflushed_count.load(SeqCst);

        println!("Completeness: {}, Unflushed: {}, Throughput: {}, Data age (seconds): {:?}, Raw data size: {} bytes, Data flushed: {} bytes", completeness, unflushed_count, rate, delay.as_secs_f64(), raw_data_size, bytes_flushed);
        thread::sleep(interval);
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

type Sample<'s, I> = (I, u64, &'s [SampleType]);
type SampleOwned<I> = (I, u64, Vec<SampleType>);

fn get_last_kafka_timestamp(topic: &str, bootstraps: &str) -> usize {
    let mut client = KafkaClient::new(bootstraps.split(',').map(String::from).collect());
    client.load_metadata_all().unwrap();

    // move client to latest offsets
    let fetch_offset = FetchOffset::Latest;
    let offsets = client.fetch_offsets(&[topic], fetch_offset).unwrap();
    let mut consumer = Consumer::from_client(client)
        .with_topic(String::from(topic))
        .with_fetch_max_bytes_per_partition(10_000_000)
        .create()
        .unwrap();
    for (_topic, partition_offsets) in offsets.iter() {
        for po in partition_offsets {
            consumer
                .consume_message(topic, po.partition, po.offset)
                .unwrap();
        }
    }

    let mut buffer = vec![0u8; 500_000_000];
    let mut max_ts = 0;
    for set in consumer.poll().unwrap().iter() {
        let _p = set.partition();
        for msg in set.messages().iter() {
            let data = decompress_kafka_msg(msg, buffer.as_mut_slice());
            let ts = data.last().unwrap().1 as usize;
            max_ts = max_ts.max(ts);
        }
    }
    max_ts
}

#[allow(dead_code)]
fn mach_query(series: Series) -> Option<usize> {
    let snapshot = series.snapshot();
    let mut snapshot = snapshot.into_iterator();
    snapshot.next_segment().unwrap();
    let seg = snapshot.get_segment();
    let mut timestamps = seg.timestamps().iterator();
    let ts: usize = timestamps.next_timestamp()? as usize;
    let now: usize = micros_from_epoch().try_into().unwrap();
    Some(now - ts)
}

fn init_mach_querier() {
    //let consumer_offset = ConsumerOffset::Latest;
    //let mut consumer = BufferedConsumer::new(BOOTSTRAPS, TOPIC);
    let snapshotter = MACH.lock().unwrap().init_snapshotter();
    //let mut kafka_client = Client::new(BOOTSTRAPS);
    let snapshotter_id = snapshotter.initialize_snapshotter(
        SERIES_IDS[0],
        Duration::from_millis(500),
        Duration::from_secs(300),
    );
    thread::sleep(Duration::from_secs(2));
    loop {
        //let start = Instant::now();
        let now: usize = micros_from_epoch().try_into().unwrap();
        let offset = snapshotter.get(snapshotter_id).unwrap();
        let mut snapshot = offset.load().into_iterator();
        //let mut snapshot = snapshotter.get(snapshotter_id).unwrap().as_ref().clone().into_iterator(&mut consumer);
        snapshot.next_segment().unwrap();
        let seg = snapshot.get_segment();
        let mut timestamps = seg.timestamps().iterator();
        let ts: usize = timestamps.next_timestamp().unwrap().try_into().unwrap();
        //println!("Query time: {:?}", start.elapsed());
        COUNTERS.data_age.store(now - ts, SeqCst);
        thread::sleep(Duration::from_secs(1));
    }
}

fn init_kafka_consumer() {
    loop {
        let topic = ARGS.kafka_topic.clone();
        let bootstraps = ARGS.kafka_bootstraps.clone();
        //let latest_timestamp = latest_timestamp.clone();
        std::thread::spawn(move || {
            //let _start = Instant::now();
            let max_ts = get_last_kafka_timestamp(topic.as_str(), bootstraps.as_str());
            let now: usize = micros_from_epoch().try_into().unwrap();
            if max_ts > 0 {
                COUNTERS.data_age.store(now - max_ts, SeqCst);
            }
            //println!("Query time: {:?}", start.elapsed());
        });
        thread::sleep(Duration::from_secs(1));
    }
}

struct Writer<I> {
    sender: Sender<Vec<Sample<'static, I>>>,
    barrier: Arc<Barrier>,
}

impl<I> Writer<I> {
    fn done(self) {
        drop(self.sender);
        self.barrier.wait();
    }
}

fn kafka_writer(barrier: Arc<Barrier>, receiver: Receiver<Vec<Sample<SeriesId>>>) {
    //let topic = ARGS.kafka_topic.clone();
    let mut producer = Producer::new();
    //let partitions: i32 = ARGS.kafka_partitions.try_into().unwrap();
    //let mut rng = rand::thread_rng();
    while let Ok(data) = receiver.recv() {
        let bytes = bincode::serialize(&data).unwrap();
        let mut compressed: Vec<u8> = Vec::new();
        lz4::compress_to_vec(bytes.as_slice(), &mut compressed, lz4::ACC_LEVEL_DEFAULT).unwrap();
        compressed.extend_from_slice(&bytes.len().to_be_bytes()[..]); // Size of uncompressed bytes
        producer.send(compressed.as_slice());
        COUNTERS.samples_written.fetch_add(data.len(), SeqCst);
    }
    barrier.wait();
}

fn init_kafka() -> Writer<SeriesId> {
    make_topic(&ARGS.kafka_bootstraps, &ARGS.kafka_topic);
    let (tx, rx) = bounded(1);
    let barrier = Arc::new(Barrier::new(ARGS.kafka_writers + 1));

    for _ in 0..ARGS.kafka_writers {
        let barrier = barrier.clone();
        let rx = rx.clone();
        thread::spawn(move || {
            kafka_writer(barrier, rx);
        });
    }

    Writer {
        sender: tx,
        barrier,
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct ESSample {
    series_id: SeriesId,
    timestamp: u64,
    data: Vec<SampleType>,
}

impl From<SampleOwned<SeriesId>> for ESSample {
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

fn decompress_kafka_msg(msg: &Message, buffer: &mut [u8]) -> Vec<SampleOwned<SeriesId>> {
    let original_sz = usize::from_be_bytes(
        msg.value[msg.value.len() - 8..msg.value.len()]
            .try_into()
            .unwrap(),
    );
    let sz = lz4::decompress(
        &msg.value[..msg.value.len() - 8],
        &mut buffer[..original_sz],
    )
    .unwrap();
    let data: Vec<SampleOwned<SeriesId>> = bincode::deserialize(&buffer[..sz]).unwrap();
    data
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
                let data = decompress_kafka_msg(msg, buffer.as_mut_slice());
                let es_data: Vec<ESSample> = data.into_iter().map(|s| s.into()).collect();
                sender.send(es_data).unwrap();
            }
        }
    }
}

async fn es_ingest(
    index_name: &String,
    consumer: Receiver<Vec<ESSample>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let batch_size = 1024;
    let client = Elasticsearch::default();
    client
        .index(IndexParts::Index(index_name))
        .send()
        .await
        .unwrap();
    println!("index {} created", index_name);

    let mut batch_item_no = 0;
    let mut num_batches_written = 0;
    let mut body: Vec<JsonBody<_>> = Vec::with_capacity(batch_size * 2);
    while let Ok(samples) = consumer.recv() {
        for sample in samples {
            body.push(json!({"index": {"_id": batch_item_no}}).into());
            body.push(sample.into());
            batch_item_no += 1;
            if batch_item_no == batch_size {
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
                body = Vec::with_capacity(batch_size * 2);
            }
        }
    }

    if !body.is_empty() {
        let r = client
            .bulk(BulkParts::Index(index_name.as_str()))
            .body(body)
            .send()
            .await
            .unwrap();
        assert!(r.error_for_status_code().is_ok());
        num_batches_written += 1;
        println!("Num batches written: {}", num_batches_written);
    }

    Ok(())
}

fn es_ingestor(index_name: &String, consumer: Receiver<Vec<ESSample>>) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        es_ingest(index_name, consumer).await.unwrap();
    });
}

async fn es_watch_freshness(index_name: &String) {
    let one_sec = std::time::Duration::from_secs(1);
    let client = Elasticsearch::default();
    loop {
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
        let r_body = r.json::<serde_json::Value>().await.unwrap();
        match &r_body["hits"]["hits"][0]["sort"][0] {
            serde_json::Value::Null => println!("es query erred; index not ready yet?"),
            serde_json::Value::Number(ts) => {
                let ts_curr = timestamp_now();
                let ts_got = ts.as_u64().unwrap();
                println!("got ts {}, freshness: {}", ts_got, ts_curr - ts_got);
            }
            _ => unreachable!("unexpected timestamp type"),
        }
        tokio::time::sleep(one_sec).await;
    }
}

fn es_freshness_watcher(index_name: &String) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        es_watch_freshness(index_name).await;
    });
}

fn init_kafka_es() -> Writer<SeriesId> {
    make_topic(&ARGS.kafka_bootstraps, &ARGS.kafka_topic);
    let (tx, rx) = bounded(1);
    let barrier = Arc::new(Barrier::new(ARGS.kafka_writers + 1));

    for _ in 0..ARGS.kafka_writers {
        let barrier = barrier.clone();
        let rx = rx.clone();
        thread::spawn(move || {
            kafka_writer(barrier, rx);
        });
    }

    let (consume_tx, consume_rx) = bounded(10);
    thread::spawn(move || kafka_es_consumer(&ARGS.kafka_topic, &ARGS.kafka_bootstraps, consume_tx));
    thread::spawn(move || es_ingestor(&ES_INDEX_NAME, consume_rx));
    thread::spawn(move || es_freshness_watcher(&ES_INDEX_NAME));

    Writer {
        sender: tx,
        barrier,
    }
}

fn mach_writer(barrier: Arc<Barrier>, receiver: Receiver<Vec<Sample<SeriesRef>>>) {
    let mut writer_guard = MACH_WRITER.lock().unwrap();
    let writer = &mut *writer_guard;
    while let Ok(data) = receiver.recv() {
        for item in data.iter() {
            'push_loop: loop {
                if writer.push(item.0, item.1, item.2).is_ok() {
                    COUNTERS
                        .raw_data_size
                        .fetch_add(item.2[0].as_bytes().len(), SeqCst);
                    break 'push_loop;
                }
            }
        }
        COUNTERS.samples_written.fetch_add(data.len(), SeqCst);
    }
    barrier.wait();
}

fn init_mach() -> Writer<SeriesRef> {
    //let (tx, rx) = unbounded();
    let (tx, rx) = bounded(1);
    let barrier = Arc::new(Barrier::new(2));

    {
        let barrier = barrier.clone();
        let rx = rx.clone();
        thread::spawn(move || {
            mach_writer(barrier, rx);
        });
    }

    Writer {
        sender: tx,
        barrier,
    }
}

fn micros_from_epoch() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
}

#[derive(Debug, Copy, Clone)]
struct Workload {
    samples_per_second: f64,
    duration: Duration,
    //sample_interval: Duration,
    batch_interval: Duration,
    batch_size: usize,
}

impl Workload {
    fn new(samples_per_second: f64, duration: Duration) -> Self {
        let sample_interval = Duration::from_secs_f64(1.0 / samples_per_second);
        let batch_interval = sample_interval * ARGS.batch_size;
        Self {
            samples_per_second,
            duration,
            //sample_interval,
            batch_interval,
            batch_size: ARGS.batch_size as usize,
        }
    }

    fn run<I: Copy>(&self, writer: &Writer<I>, samples: &'static [(I, u64, Vec<SampleType>)]) {
        println!("Running rate: {}", self.samples_per_second);
        let start = Instant::now();
        let mut last_batch = start;
        let mut batch = Vec::with_capacity(self.batch_size);
        let mut produced_samples = 0u32;
        'outer: loop {
            for item in samples {
                let timestamp: u64 = micros_from_epoch().try_into().unwrap();
                batch.push((item.0, timestamp, item.2.as_slice()));
                if batch.len() == self.batch_size {
                    while last_batch.elapsed() < self.batch_interval {}
                    match writer.sender.try_send(batch) {
                        Ok(_) => {
                            COUNTERS.samples_enqueued.fetch_add(self.batch_size, SeqCst);
                        }
                        Err(_) => {
                            COUNTERS.samples_dropped.fetch_add(self.batch_size, SeqCst);
                        }
                    };
                    produced_samples += self.batch_size as u32;
                    batch = Vec::with_capacity(self.batch_size);
                    last_batch = std::time::Instant::now();
                    if start.elapsed() > self.duration {
                        break 'outer;
                    }
                }
            }
        }
        let produce_duration = start.elapsed().as_secs_f64();
        let produced_samples: f64 = produced_samples.try_into().unwrap();
        println!(
            "Expected rate: {}, measured rate: {}",
            self.samples_per_second,
            produced_samples / produce_duration
        );
        //counters.last_timestamp = timestamp;
    }
}

fn main() {
    //let mut counters = Counters::new();
    COUNTERS.init_watcher();
    let workloads = &[
        Workload::new(500_000., Duration::from_secs(60 * 60)),
        //Workload::new(2_000_000., Duration::from_secs(70)),
        //Workload::new(2_000_000., Duration::from_secs(60)),
        //Workload::new(500_000., Duration::from_secs(120)),
        //Workload::new(3_000_000., Duration::from_secs(60)),
        //Workload::new(500_000., Duration::from_secs(120)),
    ];
    match ARGS.tsdb.as_str() {
        "es" => {
            let samples = SAMPLES.as_slice();
            let kafka_es = init_kafka_es();
            COUNTERS.init_kafka_consumer();
            COUNTERS.start_watcher();
            for workload in workloads {
                workload.run(&kafka_es, samples);
            }
            kafka_es.done();
        }
        "kafka" => {
            let samples = SAMPLES.as_slice();
            let kafka = init_kafka();
            COUNTERS.init_kafka_consumer();
            COUNTERS.start_watcher();
            for workload in workloads {
                workload.run(&kafka, samples);
            }
            kafka.done();
        }
        "mach" => {
            let samples = MACH_SAMPLES.as_slice();
            let _ = SERIES_IDS.len();
            let mach = init_mach();
            COUNTERS.init_mach_querier();
            COUNTERS.start_watcher();
            snapshotter::initialize_snapshot_server(&mut *MACH.lock().unwrap());
            for workload in workloads {
                workload.run(&mach, samples);
            }
            mach.done();
        }
        _ => panic!(),
    }
}
