use clap::*;
use lazy_static::lazy_static;
use rand::{thread_rng, Rng};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
    producer::FutureProducer,
    producer::FutureRecord,
};
use std::thread::JoinHandle;
use std::{sync::atomic::AtomicU64, thread, time::Duration};

lazy_static! {
    static ref TOPIC: String = uuid::Uuid::new_v4().to_string();
    static ref KAFKA_PARTITIONS: i32 = 3;
    static ref KAFKA_REPLICATION: i32 = 3;
    static ref COUNTER: AtomicU64 = AtomicU64::new(0);
}

#[derive(Parser, Debug, Clone)]
struct Args {
    #[clap(short, long, default_value_t = 1)]
    writers: usize,
    #[clap(short, long, default_value_t = String::from("localhost:9093,localhost:9094,localhost:9095"))]
    bootstrap_servers: String,
    #[clap(short, long, default_value_t = 1)]
    batch_size_mb: usize,
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
        .set("message.copy.max.bytes", "5000000")
        .set("compression.type", "none")
        .set("acks", "all")
        .set("message.timeout.ms", "10000")
        .create()
        .unwrap();
    producer
}

fn make_topic(bootstrap_servers: &str) {
    let topic = TOPIC.clone();
    let client: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers.to_string())
        .create()
        .unwrap();
    let admin_opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(5)));
    let topics = &[NewTopic {
        name: topic.as_str(),
        num_partitions: *KAFKA_PARTITIONS,
        replication: TopicReplication::Fixed(*KAFKA_REPLICATION),
        config: Vec::new(),
    }];
    futures::executor::block_on(client.create_topics(topics, &admin_opts)).unwrap();
    println!("topic created: {}", TOPIC.as_str());
}

fn make_payload(num_bytes: usize) -> Vec<u8> {
    (0..num_bytes).map(|_| thread_rng().gen()).collect()
}

fn kafka_write(producer: FutureProducer, payload: &[u8]) {
    let payload_len: u64 = payload.len().try_into().unwrap();
    loop {
        let to_send: FutureRecord<str, [u8]> = FutureRecord::to(&TOPIC).payload(payload);
        futures::executor::block_on(producer.send(to_send, Duration::from_secs(0))).unwrap();
        COUNTER.fetch_add(payload_len, std::sync::atomic::Ordering::SeqCst);
    }
}

fn main() {
    let args = Args::parse();
    println!("Args:\n{:#?}", args);

    make_topic(args.bootstrap_servers.as_str());
    let payload = make_payload(args.batch_size_mb * 1_000_000);

    let writer_handles: Vec<JoinHandle<()>> = (0..args.writers)
        .map(|_| {
            let producer = default_producer(args.bootstrap_servers.clone());
            let my_payload = payload.clone();
            thread::spawn(move || kafka_write(producer, my_payload.as_slice()))
        })
        .collect();

    let print_thread = thread::spawn(print_throughput);

    for handle in writer_handles {
        handle.join().unwrap();
    }
    print_thread.join().unwrap();
}
