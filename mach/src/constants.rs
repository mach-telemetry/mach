// mem list parameters
pub const INIT_FLUSHERS: usize = 4;
pub const BLOCK_SZ: usize = 1_000_000;

// Kafka specification
pub const PARTITIONS: i32 = 4;
pub const REPLICAS: i32 = 3;
//pub const BOOTSTRAPS: &str = "localhost:9093,localhost:9094,localhost:9095";
pub const BOOTSTRAPS: &str = {
    "b-2.francocluster.gzb9aa.c17.kafka.us-east-1.amazonaws.com:9092,b-3.francocluster.gzb9aa.c17.kafka.us-east-1.amazonaws.com:9092,b-1.francocluster.gzb9aa.c17.kafka.us-east-1.amazonaws.com:9092"
};
pub const TOPIC: &str = "MACH";
pub const MAX_MSG_SZ: usize = 1_500_000;
pub const MAX_FETCH_BYTES: i32 = 1_750_000;

// Segment / Initial Buffer params
pub const HEAP_SZ: usize = 1_000_000;
pub const HEAP_TH: usize = 3 * (HEAP_SZ / 4);
pub const SEGSZ: usize = 256;

// Test utils constants
pub const UNIVARIATE: &str = "bench1_univariate_small.json";
pub const MULTIVARIATE: &str = "bench1_multivariate_small.json";
pub const LOGS: &str = "SSH.log";
pub const MIN_SAMPLES: usize = 30_000;

use std::sync::atomic::AtomicUsize;
pub static COUNTER1: AtomicUsize = AtomicUsize::new(0);
pub static COUNTER2: AtomicUsize = AtomicUsize::new(0);
pub static COUNTER3: AtomicUsize = AtomicUsize::new(0);
pub static COUNTER4: AtomicUsize = AtomicUsize::new(0);
pub static COUNTER5: AtomicUsize = AtomicUsize::new(0);
