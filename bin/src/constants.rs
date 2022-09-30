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
        vec![Workload::new(5_000_000, Duration::from_secs(60 * 2)),];
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
    pub kafka_writers: i32,

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

    /// The queue into which batches (e.g., Kafka or Mach batches) are written. If bounded, will be bounded to 1. If the queue is full, the workload will drop the batch.
    #[clap(short, long)]
    pub unbounded_queue: bool,

    /// The block size for each Kafka block written by Mach
    #[clap(long, default_value_t = 1_000_000)]
    pub mach_block_sz: usize,

    #[clap(long, default_value_t = 1)]
    pub mach_writers: usize,

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

    #[clap(long, default_value_t = 60)]
    pub query_max_delay: u64,

    #[clap(long, default_value_t = 10)]
    pub min_query_duration: u64,

    #[clap(long, default_value_t = 60)]
    pub max_query_duration: u64,

    #[clap(long, default_value_t = 1)]
    pub query_count: u64,

    #[clap(long, default_value_t = 10)]
    pub query_interval_seconds: u64,

    #[clap(long, default_value_t = 10)]
    pub query_rand_seed: u64,

    #[clap(long, default_value_t = 5)]
    pub print_interval_seconds: u64,
}

pub struct Counters {
    samples_generated: AtomicUsize,
    samples_dropped: AtomicUsize,
    bytes_generated: AtomicUsize,
    bytes_dropped: AtomicUsize,
    bytes_written_to_kafka: AtomicUsize,
}

impl Counters {
    fn new() -> Self {
        Counters {
            samples_generated: AtomicUsize::new(0),
            samples_dropped: AtomicUsize::new(0),
            bytes_generated: AtomicUsize::new(0),
            bytes_dropped: AtomicUsize::new(0),
            bytes_written_to_kafka: AtomicUsize::new(0),
        }
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

    pub fn add_bytes_dropped(&self, n: usize) {
        self.bytes_dropped.fetch_add(n, SeqCst);
    }

    pub fn bytes_dropped(&self) -> usize {
        self.bytes_dropped.load(SeqCst)
    }

    pub fn add_bytes_written_to_kafka(&self, n: usize) {
        self.bytes_written_to_kafka.fetch_add(n, SeqCst);
    }
}

// Workload
#[derive(Debug, Copy, Clone)]
pub struct Workload {
    pub samples_per_second: usize,
    pub duration: Duration,
}

impl Workload {
    fn new(samples_per_second: usize, duration: Duration) -> Self {
        Self {
            samples_per_second,
            duration,
        }
    }
}
