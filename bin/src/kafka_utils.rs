#![allow(dead_code)]

pub use kafka::client::{KafkaClient, ProduceMessage, RequiredAcks};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
};
use std::ops::{Deref, DerefMut};
use std::time::Duration;

pub struct KafkaTopicOptions {
    pub num_partitions: i32,
    pub num_replications: i32,
}

impl Default for KafkaTopicOptions {
    fn default() -> Self {
        Self {
            num_replications: 3,
            num_partitions: 3,
        }
    }
}

pub fn make_topic(bootstrap: &str, topic: &str, opts: KafkaTopicOptions) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let client: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .unwrap();
    let admin_opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(3)));
    let min_insync_replicas = opts.num_replications.to_string();
    let topics = &[NewTopic {
        name: topic,
        num_partitions: opts.num_partitions,
        replication: TopicReplication::Fixed(opts.num_replications),
        config: vec![("min.insync.replicas", min_insync_replicas.as_str())],
    }];
    rt.block_on(client.create_topics(topics, &admin_opts))
        .unwrap();
}

pub struct Producer(KafkaClient);

impl Deref for Producer {
    type Target = KafkaClient;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Producer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Producer {
    pub fn new(bootstraps: &str) -> Self {
        let bootstraps = bootstraps.split(",").map(|x| String::from(x)).collect();
        let mut client = KafkaClient::new(bootstraps);
        client.load_metadata_all().unwrap();
        Self(client)
    }

    pub fn send(&mut self, topic: &str, partition: i32, item: &[u8]) -> (i32, i64) {
        let req = &[ProduceMessage::new(topic, partition, None, Some(item))];
        let resp = self
            .0
            .produce_messages(RequiredAcks::All, Duration::from_millis(1000), req)
            .unwrap();
        let part = resp[0].partition_confirms[0].partition;
        let offset = resp[0].partition_confirms[0].offset.unwrap();
        (part, offset)
    }
}
