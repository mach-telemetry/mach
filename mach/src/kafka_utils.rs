use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
};
pub use kafka::client::{KafkaClient, ProduceMessage, RequiredAcks};
use std::ops::{Deref, DerefMut};
use std::time::Duration;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

pub static TOTAL_MB_WRITTEN: AtomicUsize = AtomicUsize::new(0);

pub fn make_topic(bootstrap: &str, topic: &str) {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let client: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .unwrap();
    let admin_opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(3)));
    let topics = &[NewTopic {
        name: topic,
        num_partitions: 3,
        replication: TopicReplication::Fixed(3),
        config: vec![("min.insync.replicas", "3")],
    }];
    rt.block_on(client.create_topics(topics, &admin_opts)).unwrap();
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
        let resp = self.0.produce_messages(RequiredAcks::All, Duration::from_millis(1000), req).unwrap();
        TOTAL_MB_WRITTEN.fetch_add(item.len(), SeqCst);
        let part = resp[0].partition_confirms[0].partition;
        let offset = resp[0].partition_confirms[0].offset.unwrap();
        (part, offset)
    }
}
