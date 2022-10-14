use crate::utils::timestamp_now_micros;
use clap::*;
use lazy_static::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering::SeqCst},
    Arc,
};
use std::time::Duration;

// PARAMETERS to toggle for evaluations
lazy_static! {
    pub static ref PARAMETERS: Args = Args::parse();
    pub static ref WORKLOAD: Vec<Workload> =
        vec![
            Workload::new(8_000_000, Duration::from_secs(60*60)),
            //Workload::new(200_000, Duration::from_secs(60 * 5)),
            //Workload::new(8_000_000, Duration::from_secs(60 * 5)),
            //Workload::new(200_000, Duration::from_secs(60 * 60)),
            //Workload::new(6_000_000, Duration::from_secs(60 * 2)),
            //Workload::new(200_000, Duration::from_secs(60 * 5)),
            //Workload::new(1_000_000, Duration::from_secs(60 * 4)),
            //Workload::new(2_000_000, Duration::from_secs(60 * 4)),
            //Workload::new(3_000_000, Duration::from_secs(60 * 4)),
            //Workload::new(4_000_000, Duration::from_secs(60 * 4)),
            //Workload::new(5_000_000, Duration::from_secs(60 * 4)),
            //Workload::new(7_000_000, Duration::from_secs(60 * 4)),
            //Workload::new(5_000_000, Duration::from_secs(60 * 2)),
            //Workload::new(6_000_000, Duration::from_secs(60 * 2)),
            //Workload::new(7_000_000, Duration::from_secs(60 * 4)),
            //Workload::new(8_000_000, Duration::from_secs(60 * 4)),
            //Workload::new(9_000_000, Duration::from_secs(60 * 4)),
            //Workload::new(10_000_000, Duration::from_secs(60 * 4)),
            //Workload::new(11_000_000, Duration::from_secs(60 * 4)),
            //Workload::new(12_000_000, Duration::from_secs(60 * 4)),
            //Workload::new(13_000_000, Duration::from_secs(60 * 4)),
            //Workload::new(14_000_000, Duration::from_secs(60 * 4)),
            //Workload::new(15_000_000, Duration::from_secs(60 * 4)),
        ];
    pub static ref COUNTERS: Arc<Counters> = Arc::new(Counters::new());
}

#[derive(Parser, Debug, Clone)]
pub struct Args {
    #[clap(long, default_value_t = 1_000_000)]
    pub kafka_batch_bytes: usize,

    //#[clap(long, default_value_t = 1)]
    //pub writer_queue_len: usize,
    #[clap(long, default_value_t = String::from("localhost:9093,localhost:9094,localhost:9095"))]
    pub kafka_bootstraps: String,

    #[clap(long, default_value_t = 4)]
    pub kafka_partitions: i32,

    #[clap(long, default_value_t = 1)]
    pub kafka_writers: u64,

    #[clap(long, default_value_t = 3)]
    pub kafka_replicas: i32,

    #[clap(long, default_value_t = String::from("kafka-completeness-bench"))]
    pub kafka_topic: String,

    #[clap(short, long, default_value_t = String::from("http://localhost:9200"))]
    pub es_endpoint: String,

    #[clap(short, long)]
    pub es_username: Option<String>,

    #[clap(short, long)]
    pub es_password: Option<String>,

    #[clap(long, default_value_t = 15_000_000)]
    pub es_batch_bytes: usize,

    #[clap(short, long, default_value_t = 10)]
    pub es_num_shards: usize,

    #[clap(short, long, default_value_t = 0)]
    pub es_num_replicas: usize,

    #[clap(short, long, default_value_t = format!("test-data-{}", timestamp_now_micros()))]
    pub es_index_name: String,

    /// The queue into which batches (e.g., Kafka or Mach batches) are written. If bounded, will be bounded to 1. If the queue is full, the workload will drop the batch.
    #[clap(short, long)]
    pub unbounded_queue: bool,

    /// The block size for each Kafka block written by Mach
    #[clap(long, default_value_t = 1_000_000)]
    pub mach_block_sz: usize,

    #[clap(long, default_value_t = 1)]
    pub mach_writers: u64,

    /// Snapshot interval for Mach when querying a source
    #[clap(long, default_value_t = 0.5)]
    pub mach_snapshot_interval: f64,

    /// Snapshot timeout for Mach after initing a source
    #[clap(long, default_value_t = 60. * 60.)]
    pub mach_snapshot_timeout: f64,

    /// The workload uses a channel to send samples to a writer. This batching amortizes the
    /// cost of writing to a channel.
    #[clap(long, default_value_t = 10_000_000)]
    pub writer_batches: usize,

    #[clap(long, default_value_t = 1000)]
    pub source_count: u64,

    #[clap(long, default_value_t = String::from("/home/fsolleza/data/intel-telemetry/processed-data.bin"))]
    pub file_path: String,

    #[clap(long, default_value_t = 5.0)]
    pub counter_interval_seconds: f64,

    #[clap(long, default_value_t = 1000)]
    pub kafka_index_size: usize,

    #[clap(long, default_value_t = String::from("127.0.0.1"))]
    pub querier_ip: String,

    #[clap(long, default_value_t = 12989)]
    pub querier_port: u16,

    #[clap(long, default_value_t = 3)]
    pub query_min_delay: u64,

    #[clap(long, default_value_t = 10)]
    pub query_max_delay: u64,

    #[clap(long, default_value_t = 10)]
    pub query_min_duration: u64,

    #[clap(long, default_value_t = 60)]
    pub query_max_duration: u64,

    #[clap(long, default_value_t = 100)]
    pub query_count: u64,

    #[clap(long, default_value_t = 10)]
    pub query_interval_seconds: u64,

    #[clap(long, default_value_t = 10)]
    pub query_rand_seed: u64,

    #[clap(long, default_value_t = 5)]
    pub print_interval_seconds: u64,

    #[clap(long, default_value_t = 1)]
    pub data_generator_count: u64,

    #[clap(long, default_value_t = 10_000_000)]
    pub max_sample_rate_per_generator: u64,

    #[clap(long, default_value_t = 10_000_000)]
    pub max_writers_per_generator: u64,

    #[clap(long, default_value_t = String::from("http://localhost:50051"))]
    pub snapshot_server_port: String,
}

pub struct Counters {
    samples_generated: AtomicUsize,
    samples_dropped: AtomicUsize,
    bytes_generated: AtomicUsize,
    bytes_written: AtomicUsize,
    bytes_written_to_kafka: AtomicUsize,
    messages_written_to_kafka: AtomicUsize,
    current_workload_rate: AtomicUsize,
}

impl Counters {
    fn new() -> Self {
        Counters {
            samples_generated: AtomicUsize::new(0),
            samples_dropped: AtomicUsize::new(0),
            bytes_generated: AtomicUsize::new(0),
            bytes_written: AtomicUsize::new(0),
            messages_written_to_kafka: AtomicUsize::new(0),
            bytes_written_to_kafka: AtomicUsize::new(0),
            current_workload_rate: AtomicUsize::new(0),
        }
    }

    pub fn set_current_workload_rate(&self, n: usize) {
        self.current_workload_rate.store(n, SeqCst);
    }

    pub fn current_workload_rate(&self) -> usize {
        self.current_workload_rate.load(SeqCst)
    }

    pub fn add_samples_generated(&self, n: usize) {
        self.samples_generated.fetch_add(n, SeqCst);
    }

    pub fn samples_generated(&self) -> usize {
        self.samples_generated.load(SeqCst)
    }

    pub fn add_samples_dropped(&self, n: usize) {
        self.samples_dropped.fetch_add(n, SeqCst);
    }

    pub fn samples_dropped(&self) -> usize {
        self.samples_dropped.load(SeqCst)
    }

    pub fn add_bytes_generated(&self, n: usize) {
        self.bytes_generated.fetch_add(n, SeqCst);
    }

    pub fn bytes_generated(&self) -> usize {
        self.bytes_generated.load(SeqCst)
    }

    pub fn add_bytes_written(&self, n: usize) {
        self.bytes_written.fetch_add(n, SeqCst);
    }

    pub fn bytes_written(&self) -> usize {
        self.bytes_written.load(SeqCst)
    }

    pub fn add_bytes_written_to_kafka(&self, n: usize) {
        self.bytes_written_to_kafka.fetch_add(n, SeqCst);
    }

    pub fn bytes_written_to_kafka(&self) -> usize {
        self.bytes_written_to_kafka.load(SeqCst)
    }

    pub fn add_messages_written_to_kafka(&self, n: usize) {
        self.messages_written_to_kafka.fetch_add(n, SeqCst);
    }

    pub fn messages_written_to_kafka(&self) -> usize {
        self.messages_written_to_kafka.load(SeqCst)
    }
}

// Workload
#[derive(Debug, Copy, Clone)]
pub struct Workload {
    pub samples_per_second: u64,
    pub duration: Duration,
}

impl Workload {
    pub fn new(samples_per_second: u64, duration: Duration) -> Self {
        Self {
            samples_per_second,
            duration,
        }
    }

    pub fn split_rate(&self, n: u64) -> Vec<Self> {
        let samples_per_second = self.samples_per_second / n;
        let excess = self.samples_per_second % n;
        let mut result = Vec::new();
        for _ in 0..n {
            let w = Workload {
                samples_per_second,
                duration: self.duration,
            };
            result.push(w);
        }
        result[n as usize - 1].samples_per_second += excess;
        result
    }
}
