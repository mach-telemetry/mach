mod otlp;

use clap::*;
use rdkafka::{
    config::ClientConfig,
    consumer::{stream_consumer::StreamConsumer, CommitMode, Consumer, DefaultConsumerContext},
    Message,
};
use mach::{
    utils::random_id,
    active_block::StaticBlock,
};
use zstd::stream::decode_all;

#[derive(Parser, Debug, Clone)]
struct Args {
    #[clap(short, long, default_value_t = String::from("localhost:9093,localhost:9094,localhost:9095"))]
    kafka_bootstraps: String,

    #[clap(short, long)]
    kafka_topic: String,
}

async fn mach_consumer(args: Args) {
    let consumer: StreamConsumer<DefaultConsumerContext> = ClientConfig::new()
        .set("bootstrap.servers", &args.kafka_bootstraps)
        .set("group.id", random_id())
        .create()
        .unwrap();
    consumer
        .subscribe(&[&args.kafka_topic])
        .expect("Can't subscribe to specified topics");

    println!("Reading data");

    loop {
        match consumer.recv().await {
            Err(e) => println!("Kafka error: {}", e),
            Ok(m) => {
                match m.payload_view::<[u8]>() {
                    None => {}
                    Some(Ok(s)) => {
                        let sz = s.len();
                        // we use zstd inside mach to compress the block before writing to kafka
                        let block = StaticBlock::new(decode_all(s).unwrap());
                        let count = block.samples();
                        println!("Block size: {}, sample count: {}", sz, count);
                    }
                    Some(Err(_)) => {
                        println!("Error while deserializing message payload");
                    }
                };
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    println!("Args: {:?}", args);
    mach_consumer(args).await;
}


