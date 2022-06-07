use clap::*;
use lazy_static::lazy_static;
use rdkafka::{config::ClientConfig, producer::FutureProducer, producer::FutureRecord};
use std::thread::JoinHandle;
use std::{sync::atomic::AtomicU64, thread, time::Duration};

lazy_static! {
    static ref BOOTSTRAP_SERVERS: String =
        String::from("localhost:9093,localhost:9094,localhost:9095");
    static ref TOPIC: String = uuid::Uuid::new_v4().to_string();
    static ref PAYLOAD: [u8; 1_000_000] = [33; 1_000_000];
    static ref COUNTER: AtomicU64 = AtomicU64::new(0);
}

#[derive(Parser, Debug, Clone)]
struct Args {
    #[clap(short, long, default_value_t = 1)]
    writers: usize,
}

fn print_throughput() {
    let mut prev = 0;
    loop {
        let curr = COUNTER.load(std::sync::atomic::Ordering::SeqCst);
        let bytes_sent = curr - prev;
        prev = curr;
        println!("Throughput: {} bytes / sec", bytes_sent);
        thread::sleep(Duration::from_secs(1));
    }
}

fn default_producer(bootstraps: String) -> FutureProducer {
    println!("making producer to bootstraps: {}", bootstraps);
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstraps)
        .set("message.max.bytes", "1000000000")
        .set("linger.ms", "0")
        .set("message.copy.max.bytes", "5000000")
        .set("batch.num.messages", "1")
        .set("compression.type", "none")
        .set("acks", "all")
        .set("message.timeout.ms", "10000")
        .create()
        .unwrap();
    producer
}

fn kafka_write() {
    let producer = default_producer(BOOTSTRAP_SERVERS.to_string());
    loop {
        let to_send: FutureRecord<str, [u8]> =
            FutureRecord::to(&TOPIC).payload(&PAYLOAD.as_slice());
        futures::executor::block_on(producer.send(to_send, Duration::from_secs(0))).unwrap();
        COUNTER.fetch_add(
            PAYLOAD.len().try_into().unwrap(),
            std::sync::atomic::Ordering::SeqCst,
        );
    }
}

fn main() {
    let args = Args::parse();
    println!("Args:\n{:#?}", args);

    let writer_handles: Vec<JoinHandle<()>> = (0..args.writers)
        .map(|_| thread::spawn(kafka_write))
        .collect();

    let print_thread = thread::spawn(print_throughput);

    for handle in writer_handles {
        handle.join().unwrap();
    }
    print_thread.join().unwrap();
}
