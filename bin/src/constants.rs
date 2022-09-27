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
    pub static ref WORKLOAD: Vec<Workload> = { vec![Workload::new(500, Duration::from_secs(60)),] };
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

    #[clap(long, default_value_t = 3)]
    pub kafka_replicas: i32,

    #[clap(long, default_value_t = String::from("kafka-completeness-bench"))]
    pub kafka_topic: String,

    /// The queue into which batches (e.g., Kafka or Mach batches) are written. If bounded, will be bounded to 1. If the queue is full, the workload will drop the batch.
    #[clap(short, long)]
    pub bounded_queue: bool,

    /// The block size for each Kafka block written by Mach
    #[clap(long, default_value_t = 1_000_000)]
    pub mach_block_sz: usize,

    #[clap(long, default_value_t = 1)]
    pub mach_writers: usize,

    /// The workload uses a channel to send samples to a writer. This batching amortizes the
    /// cost of writing to a channel.
    #[clap(long, default_value_t = 1_000)]
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
}

pub struct Counters {
    samples_generated: AtomicUsize,
    samples_written: AtomicUsize,
    bytes_generated: AtomicUsize,
    bytes_written: AtomicUsize,
    bytes_written_to_kafka: AtomicUsize,
}

impl Counters {
    fn new() -> Self {
        Counters {
            samples_generated: AtomicUsize::new(0),
            samples_written: AtomicUsize::new(0),
            bytes_generated: AtomicUsize::new(0),
            bytes_written: AtomicUsize::new(0),
            bytes_written_to_kafka: AtomicUsize::new(0),
        }
    }

    pub fn add_samples_generated(&self, n: usize) {
        self.samples_generated.fetch_add(n, SeqCst);
    }

    pub fn samples_generated(&self) -> usize {
        self.samples_generated.load(SeqCst)
    }

    pub fn add_samples_written(&self, n: usize) {
        self.samples_written.fetch_add(n, SeqCst);
    }

    pub fn samples_written(&self) -> usize {
        self.samples_written.load(SeqCst)
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
}

// Workload
#[derive(Debug, Copy, Clone)]
pub struct Workload {
    pub mbps: u32,
    pub duration: Duration,
}

impl Workload {
    fn new(mbps: u32, duration: Duration) -> Self {
        Self { mbps, duration }
    }
}
