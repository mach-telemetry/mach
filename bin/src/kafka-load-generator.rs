mod kafka_utils;

use clap::*;
use kafka_utils::{make_topic, Producer};
use lazy_static::lazy_static;
use num_format::{Locale, ToFormattedString};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use std::{
    sync::atomic::{AtomicU64, Ordering::SeqCst},
    thread,
    time::{Duration, Instant},
};
use crossbeam::channel::{Sender, Receiver, bounded};

lazy_static! {
    static ref BYTES: Box<[u8]> = vec![8u8; 1_000_000].into_boxed_slice();
    static ref ARGS: Args = Args::parse();
}

#[derive(Parser, Debug, Clone)]
struct Args {
    #[clap(short, long, default_value_t = 4)]
    writer_count: i32,
    #[clap(short, long, default_value_t = 4)]
    kafka_partitions: i32,
    #[clap(short, long, default_value_t = String::from("localhost:9093,localhost:9094,localhost:9095"))]
    bootstrap_servers: String,
    /// Rate in mbps
    #[clap(short, long, default_value_t = 100)]
    rate: u64,
    #[clap(short, long, default_value_t = random_topic())]
    topic: String,
}

fn random_topic() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(7)
        .map(char::from)
        .collect()
}

fn kafka_writer(rx: Receiver<()>) {
    let mut rng = thread_rng();
    let mut producer = Producer::new(ARGS.bootstrap_servers.as_str());
    let payload = &BYTES[..];
    loop {
        if let Ok(x) = rx.try_recv() {
            let partition = rng.gen_range(0..ARGS.kafka_partitions);
            producer.send(&ARGS.topic, partition, payload);
        }
    }
}

fn init_kafka_loader() -> Sender<()> {
    let (tx, rx) = bounded(ARGS.rate as usize);
    for _ in 0..ARGS.writer_count {
        let rx = rx.clone();
        thread::spawn(move || kafka_writer(rx));
    }
    tx
}


fn main() {
    let opts = kafka_utils::KafkaTopicOptions {
        num_partitions: ARGS.kafka_partitions,
        num_replicas: 3,
    };
    make_topic(ARGS.bootstrap_servers.as_str(), ARGS.topic.as_str(), opts);


    let mut sender = init_kafka_loader();

    let mut counter = 0;
    loop {
        let now = Instant::now();
        for _ in 0..ARGS.rate {
            if sender.try_send(()).is_ok() {
                counter += 1;
            }
        }
        while now.elapsed() < Duration::from_secs(1) {}
        println!("Sent {} mb", counter);
        counter = 0;
    }
}