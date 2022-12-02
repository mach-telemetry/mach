mod kafka_entry;
mod producer;
pub use kafka_entry::KafkaEntry;
pub use producer::Producer;

use crate::constants::*;
use log::info;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
};
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::time::Duration;

static INITED: AtomicBool = AtomicBool::new(false);

pub fn init() {
    if !INITED.load(SeqCst) {
        info!(
            "Initing Kafka Connection\nbootstraps: {}\ntopic: {}\nreplicas:{}\npartitions:{}",
            BOOTSTRAPS, TOPIC, REPLICAS, PARTITIONS
        );
        make_topic();
        INITED.store(true, SeqCst);
    }
}

pub fn make_topic() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let client: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", BOOTSTRAPS)
        .create()
        .unwrap();
    let admin_opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(3)));
    let replicas = format!("{}", REPLICAS);
    let topics = &[NewTopic {
        name: TOPIC,
        num_partitions: PARTITIONS,
        replication: TopicReplication::Fixed(REPLICAS),
        config: vec![("min.insync.replicas", replicas.as_str())],
    }];
    rt.block_on(client.create_topics(topics, &admin_opts))
        .unwrap();
}
