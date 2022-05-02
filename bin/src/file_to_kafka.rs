mod sample;

use mach::{
    compression::{CompressFn, Compression},
    durable_queue::{KafkaConfig, QueueConfig},
    id::{SeriesId, WriterId},
    reader::{ReadResponse, ReadServer},
    sample::Type,
    series::{SeriesConfig, Types},
    tags::Tags,
    tsdb::Mach,
    utils::{bytes::Bytes, random_id},
    writer::{Writer, WriterConfig},
};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
    consumer::{
        CommitMode, base_consumer::BaseConsumer, stream_consumer::StreamConsumer, Consumer,
        DefaultConsumerContext,
    },
    error::KafkaError as RdKafkaError,
    producer::{FutureProducer, FutureRecord},
    topic_partition_list::{Offset, TopicPartitionList},
    types::RDKafkaErrorCode,
    util::Timeout,
    Message,
};
use serde::*;
use serde_json;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering::SeqCst},
    Arc,
};
use std::thread;
use std::time::Duration;
use tokio::fs::{self, *};
use tokio::io::{self, *};

#[tokio::main]
async fn main() {
    let producer_counter = Arc::new(AtomicUsize::new(0));
    let pc = counter.clone();

    tokio::task::spawn(async move {
        let mut last = cc.load(SeqCst);
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let cur = pc.load(SeqCst);
            println!("Producer Rate {} / sec", cur - last);
            last = cur;
        }
    });

    //tokio::task::spawn(async move {
    //    let mut last = cc.load(SeqCst);
    //    loop {
    //        tokio::time::sleep(Duration::from_secs(1)).await;
    //        let cur = cc.load(SeqCst);
    //        println!("Rate {} / sec", cur - last);
    //        last = cur;
    //    }
    //});

    // Setup topic, producer, and consumer
    let bootstraps = String::from("b-3.demo-cluster-1.c931w3.c25.kafka.us-east-1.amazonaws.com:9092,b-2.demo-cluster-1.c931w3.c25.kafka.us-east-1.amazonaws.com:9092,b-1.demo-cluster-1.c931w3.c25.kafka.us-east-1.amazonaws.com:9092");
    //let bootstraps = String::from("localhost:9093,localhost:9094,localhost:9095");
    let topic = random_id();
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let batch_size = 8129*2;

    println!("creating topic: {}", topic);
    let client: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &bootstraps)
        .create().unwrap();
    let admin_opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(3)));
    let topics = &[NewTopic {
        name: topic.as_str(),
        num_partitions: 1,
        replication: TopicReplication::Fixed(3),
        config: Vec::new()
    }];
    client.create_topics(topics, &admin_opts).await.unwrap();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstraps)
        .set("acks", "all")
        .set("message.max.bytes", "1000000000")
        .set("linger.ms", "0")
        .set("message.copy.max.bytes", "5000000")
        .set("message.timeout.ms", "3000")
        .create()
        .unwrap();

    let producer_topic = topic.clone();
    let producer_barrier = barrier.clone();
    tokio::task::spawn(async move {
        println!("Loading data");
        let reader = BufReader::new(
            File::open("/home/ubuntu/demo_data")
                .await
                .unwrap(),
        );

        let mut data: Vec<sample::Sample> = Vec::new();
        let mut lines = reader.lines();
        while let Some(x) = lines.next_line().await.unwrap() {
            let sample: sample::Sample = serde_json::from_str(&x).unwrap();
            data.push(sample);
        }

        // Write data to single writer
        println!("WAITING FOR CONSUMER");
        producer_barrier.wait().await;
        println!("Writing data");
        //let mut values = Vec::new();

        let mut buf = Vec::new();
        for mut sample in data.drain(..) {
            buf.push(sample);
            if buf.len() > 0 && buf.len() % batch_size == 0 {
                let bytes = bincode::serialize(&buf).unwrap();
                let to_send: FutureRecord<str, [u8]> = FutureRecord::to(&producer_topic).payload(&bytes);
                match producer.send(to_send, Duration::from_secs(3)).await {
                    Ok(x) => {
                        assert_eq!(x.0, 0);
                        counter.fetch_add(batch_size, SeqCst);
                    }
                    Err(x) => println!("Produce error"),
                }
                buf.clear();
            }
        }
    });

    let consumer: StreamConsumer<DefaultConsumerContext> = ClientConfig::new()
        .set("bootstrap.servers", &bootstraps)
        .set("group.id", random_id())
        .create()
        .unwrap();

    println!("WAITING FOR PRODUCER");
    barrier.wait().await;
    println!("Reading data");

    consumer.subscribe(&[&topic]).expect("Can't subscribe to specified topics");

    loop {
        match consumer.recv().await {
            Err(e) => println!("Kafka error: {}", e),
            Ok(m) => {
                match m.payload_view::<[u8]>() {
                    None => {},
                    Some(Ok(s)) => {
                        if let Ok(x) = bincode::deserialize::<Vec<sample::Sample>>(s) {
                            //counter.fetch_add(batch_size, SeqCst);
                        }
                    },
                    Some(Err(e)) => {
                        println!("Error while deserializing message payload");
                    }
                };
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        }
    }
}
