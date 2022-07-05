use crate::utils::random_id;
use dashmap::DashMap;
pub use kafka::client::{FetchOffset, FetchPartition, KafkaClient, ProduceMessage, RequiredAcks};
use kafka::consumer::{Consumer, GroupOffsetStorage};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
};
use std::ops::{Deref, DerefMut};
use std::sync::{
    atomic::{AtomicUsize, Ordering::SeqCst},
    Arc,
};
use std::time::Duration;

pub static TOTAL_MB_WRITTEN: AtomicUsize = AtomicUsize::new(0);

const PARTITIONS: i32 = 3;
const REPLICAS: i32 = 3;

pub fn make_topic(bootstrap: &str, topic: &str) {
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
        let bootstraps = bootstraps.split(',').map(String::from).collect();
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
        TOTAL_MB_WRITTEN.fetch_add(item.len(), SeqCst);
        let part = resp[0].partition_confirms[0].partition;
        let offset = resp[0].partition_confirms[0].offset.unwrap();
        //println!("PRODUCING TO KAFKA {} {}", part, offset);
        (part, offset)
    }
}

type InnerDict = Arc<DashMap<(i32, i64), Arc<[u8]>>>;

pub struct BufferedConsumer {
    data: InnerDict,
    client: KafkaClient,
    topic: String,
    prefetcher: crossbeam::channel::Sender<(i32, i64)>,
}

impl BufferedConsumer {
    pub fn new(bootstraps: &str, topic: &str, from: FetchOffset) -> Self {
        let data = Arc::new(DashMap::new());
        let (prefetcher, rx) = crossbeam::channel::unbounded();
        let mut client = KafkaClient::new(bootstraps.split(',').map(String::from).collect());
        client.load_metadata_all().unwrap();

        init_consumer_worker(bootstraps, topic, data.clone(), from);
        init_prefetcher(bootstraps, topic, data.clone(), rx);

        Self {
            data,
            client,
            topic: topic.into(),
            prefetcher,
        }
    }

    pub fn get(&mut self, partition: i32, offset: i64) -> Arc<[u8]> {
        if let Some(x) = self.data.get(&(partition, offset)) {
            x.clone()
        } else {
            self.prefetcher.send((partition, offset)).unwrap();
            let reqs = &[FetchPartition::new(self.topic.as_str(), partition, offset)
                .with_max_bytes(2_000_000)];
            let resps = self.client.fetch_messages(reqs).unwrap();
            let msg = &resps[0].topics()[0].partitions()[0]
                .data()
                .unwrap()
                .messages()[0];
            let res: Arc<[u8]> = msg.value.into();
            self.data.insert((partition, offset), res.clone());
            res
        }
    }
}

fn init_prefetcher(
    bootstraps: &str,
    topic: &str,
    data: InnerDict,
    recv: crossbeam::channel::Receiver<(i32, i64)>,
) {
    let bootstraps = bootstraps.split(',').map(String::from).collect();
    let mut client = KafkaClient::new(bootstraps);
    client.load_metadata_all().unwrap();
    let topic: String = topic.into();
    std::thread::spawn(move || {
        let mut reqs = Vec::new();
        while let Ok((part, o)) = recv.recv() {
            let start = if o < 10 { 0 } else { o - 10 };
            for offset in start..=o {
                reqs.push(
                    FetchPartition::new(topic.as_str(), part, offset).with_max_bytes(2_000_000),
                );
            }
            let resps = client.fetch_messages(&reqs).unwrap();
            for msg in resps[0].topics()[0].partitions()[0]
                .data()
                .unwrap()
                .messages()
            {
                data.insert((part, msg.offset), msg.value.into());
            }
            reqs.clear();
        }
    });
}

fn init_consumer_worker(bootstraps: &str, topic: &str, data: InnerDict, from: FetchOffset) {
    let bootstraps = bootstraps.split(',').map(String::from).collect();

    println!("OFFSET {:?}", from);
    let mut consumer = Consumer::from_hosts(bootstraps)
        .with_topic(topic.to_owned())
        .with_fallback_offset(FetchOffset::Latest)
        .with_group(random_id())
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .with_fetch_max_bytes_per_partition(2_000_000)
        .create()
        .unwrap();

    std::thread::spawn(move || loop {
        for ms in consumer.poll().unwrap().iter() {
            let partition = ms.partition();
            for m in ms.messages() {
                println!("CONSUMING FROM KAFKA: {} {}", partition, m.offset);
                data.insert((partition, m.offset), m.value.into());
            }
        }
    });
}
