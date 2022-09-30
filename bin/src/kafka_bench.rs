mod kafka_utils;

use clap::*;
use kafka_utils::{make_topic, Producer};
use lazy_static::lazy_static;
use num_format::{Locale, ToFormattedString};
use rand::{thread_rng, Rng};
use std::{
    sync::atomic::{AtomicU64, Ordering::SeqCst},
    thread,
    time::{Duration, Instant},
};

macro_rules! mb_to_bytes {
    ($mb: expr) => {
        $mb * 1_000_000
    };
}

lazy_static! {
    static ref TOPIC: String = uuid::Uuid::new_v4().to_string();
    static ref BYTES_WRITTEN: AtomicU64 = AtomicU64::new(0);
}

#[derive(Parser, Debug, Clone)]
struct Args {
    #[clap(short, long, default_value_t = 3)]
    writer_partitions: i32,
    #[clap(short, long, default_value_t = 3)]
    replicas: i32,
    #[clap(short, long, default_value_t = String::from("localhost:9093,localhost:9094,localhost:9095"))]
    bootstrap_servers: String,
    /// Number of bytes in a batch written to kafka.
    #[clap(short, long, default_value_t = mb_to_bytes!(1))]
    batch_size: usize,
    /// Total number of bytes written to kafka.
    #[clap(short, long, default_value_t = mb_to_bytes!(50_000))]
    total_bytes: usize,
}

fn collect_median(threshold: usize) -> u64 {
    let mut prev = 0;
    let mut collected = Vec::new();
    loop {
        let curr = BYTES_WRITTEN.load(SeqCst);

        let bytes_sent = curr - prev;
        prev = curr;
        println!(
            "Throughput: {} bytes / sec",
            bytes_sent.to_formatted_string(&Locale::en)
        );
        collected.push(bytes_sent);

        if curr >= threshold.try_into().unwrap() {
            collected.sort();
            return collected[collected.len() / 2];
        } else {
            thread::sleep(Duration::from_secs(1));
        }
    }
}

fn make_payload(num_bytes: usize) -> Vec<u8> {
    (0..num_bytes).map(|_| thread_rng().gen()).collect()
}

fn kafka_write(mut producer: Producer, partition: i32, payload: &[u8]) {
    let sync_interval = Duration::from_secs(1);
    let mut last_synced = Instant::now();
    let mut bytes_sent = 0;

    let payload_len: u64 = payload.len().try_into().unwrap();

    loop {
        producer.send(&TOPIC, partition, payload);
        bytes_sent += payload_len;
        if last_synced.elapsed() > sync_interval {
            BYTES_WRITTEN.fetch_add(bytes_sent, SeqCst);
            last_synced = Instant::now();
            bytes_sent = 0;
        }
    }
}

fn main() {
    let args = Args::parse();
    println!("Args:\n{:#?}", args);

    let opts = kafka_utils::KafkaTopicOptions {
        num_partitions: args.writer_partitions,
        num_replicas: args.replicas,
    };
    make_topic(args.bootstrap_servers.as_str(), TOPIC.as_str(), opts);

    let payload = make_payload(args.batch_size);

    for partition in 0..args.writer_partitions {
        let producer = Producer::new(args.bootstrap_servers.as_str());
        let my_payload = payload.clone();
        thread::spawn(move || kafka_write(producer, partition, my_payload.as_slice()));
    }

    let median = collect_median(args.total_bytes);
    println!(
        "Median: {} bytes/sec",
        median.to_formatted_string(&Locale::en)
    );
}
