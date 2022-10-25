mod producer;
mod kafka_entry;
pub use kafka_entry::KafkaEntry;

use crate::constants::*;
use kafka::{
    client::{fetch::Response, FetchPartition, KafkaClient, RequiredAcks},
    consumer::GroupOffsetStorage,
    producer::{Producer as OgProducer, Record},
};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
};
use std::time::Duration;

pub fn make_topic(bootstrap: &str, topic: &str) {
    println!("BOOTSTRAPS: {}", bootstrap);
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let client: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .unwrap();
    let admin_opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(3)));
    let replicas = format!("{}", REPLICAS);
    let topics = &[NewTopic {
        name: topic,
        num_partitions: PARTITIONS,
        replication: TopicReplication::Fixed(REPLICAS),
        config: vec![("min.insync.replicas", replicas.as_str())],
    }];
    rt.block_on(client.create_topics(topics, &admin_opts))
        .unwrap();
}
