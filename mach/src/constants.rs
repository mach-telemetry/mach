pub const KAFKA_TOPIC: &str = "MACHSTORAGE";
pub const MACH_METADATA: &str = "mach_metadata";
pub const BUFSZ: usize = 1_000_000;
pub const KAFKA_BOOTSTRAP: &str = "b-1.demo-cluster-1.c931w3.c25.kafka.us-east-1.amazonaws.com:9092,b-3.demo-cluster-1.c931w3.c25.kafka.us-east-1.amazonaws.com:9092,b-2.demo-cluster-1.c931w3.c25.kafka.us-east-1.amazonaws.com:9092";
//pub const KAFKA_BOOTSTRAP: &str = "localhost:9093,localhost:9094,localhost:9095";
pub const SEGSZ: usize = 256;
pub const REDIS_ADDR: &str = "redis://127.0.0.1/";

pub const KUBE_NAMESPACE: &str = "mach-daemon";
pub const KUBE_PORT: u16 = 21822;
pub const FILEBACKEND_DIR: &str = "/home/fsolleza/data/mach/out";
pub const ACTIVE_BLOCK_FLUSH_SZ: usize = 1_000_000;
