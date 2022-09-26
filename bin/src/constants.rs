use lazy_static::*;
use clap::*;
use std::time::{Duration};

// PARAMETERS to toggle for evaluations
lazy_static! {
    pub static ref PARAMETERS: Args = Args::parse();
    pub static ref WORKLOAD: Vec<Workload> = {
        vec![
            Workload::new(500, Duration::from_secs(60)),
        ]
    };
}

#[derive(Parser, Debug, Clone)]
pub struct Args {

    #[clap(short, long, default_value_t = 1_000_000)]
    pub kafka_batch_bytes: usize,

    //#[clap(short, long, default_value_t = 1)]
    //pub writer_queue_len: usize,

    #[clap(short, long, default_value_t = String::from("localhost:9093,localhost:9094,localhost:9095"))]
    pub kafka_bootstraps: String,

    #[clap(short, long, default_value_t = 4)]
    pub kafka_partitions: i32,

    #[clap(short, long, default_value_t = 3)]
    pub kafka_replicas: i32,

    #[clap(short, long, default_value_t = String::from("kafka-completeness-bench"))]
    pub kafka_topic: String,

    //#[clap(short, long, default_value_t = 1_000_000)]
    //pub batch_bytes: usize,

    #[clap(short, long)]
    pub bounded_queue: bool,

    #[clap(short, long, default_value_t = 1_000_000)]
    pub mach_block_sz: usize,

    #[clap(short, long, default_value_t = 1000)]
    pub source_count: u64,

    #[clap(short, long, default_value_t = String::from("/home/fsolleza/data/intel-telemetry/processed-data.bin"))]
    pub file_path: String,

    #[clap(short, long, default_value_t = 5.0)]
    pub counter_interval_seconds: f64,
}

// counters
use std::sync::atomic::AtomicUsize;

#[allow(dead_code)]
pub static SAMPLES_GENERATED: AtomicUsize = AtomicUsize::new(0);
#[allow(dead_code)]
pub static SAMPLES_INGESTED: AtomicUsize = AtomicUsize::new(0);
#[allow(dead_code)]
pub static BYTES_GENERATED: AtomicUsize = AtomicUsize::new(0);
#[allow(dead_code)]
pub static BYTES_FLUSHED: AtomicUsize = AtomicUsize::new(0);

// Workload
#[derive(Debug, Copy, Clone)]
pub struct Workload {
    pub mbps: u32,
    pub duration: Duration,
}

impl Workload {
    fn new(mbps: u32, duration: Duration) -> Self {
        Self {
            mbps,
            duration,
        }
    }
}

