use crate::utils::random_id;
use dashmap::DashMap;
use kafka::client::{FetchOffset, FetchPartition, KafkaClient, ProduceMessage, RequiredAcks};
use kafka::consumer::{Consumer, GroupOffsetStorage};
use kafka::producer::{Producer as OgProducer, Record};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
    consumer::{Consumer as RdKConsumer, DefaultConsumerContext, BaseConsumer},
    topic_partition_list::{TopicPartitionList, Offset},
    util::Timeout,
    Message,
};
use std::ops::{Deref, DerefMut};
use std::sync::{
    atomic::{AtomicUsize, Ordering::SeqCst},
    Arc,
};
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use std::convert::TryInto;
use rand::{Rng, thread_rng};

pub static TOTAL_MB_WRITTEN: AtomicUsize = AtomicUsize::new(0);

const PARTITIONS: i32 = 3;
const REPLICAS: i32 = 3;
pub const BOOTSTRAPS: &str = "localhost:9093,localhost:9094,localhost:9095";
pub const TOPIC: &str = "MACH";

pub struct Client(KafkaClient);

impl std::ops::Deref for Client {
    type Target = KafkaClient;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Client {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Client {
    pub fn new(bootstraps: &str) -> Self {
        let bootstraps = bootstraps.split(',').map(String::from).collect();
        let mut client = KafkaClient::new(bootstraps);
        Self(client)
    }

    pub fn load(&mut self, topic: &str, partition: i32, offset: i64, max_bytes: usize) -> Arc<[u8]> {
        let mut topic_partition_list = TopicPartitionList::new();
        topic_partition_list.add_partition_offset(topic, partition, Offset::Offset(offset)).unwrap();
        let consumer: BaseConsumer<DefaultConsumerContext> = ClientConfig::new()
            .set("bootstrap.servers", BOOTSTRAPS)
            .set("group.id", "random_consumer")
            .create().unwrap();
        consumer.assign(&topic_partition_list).unwrap();
        loop {
            match consumer.poll(Timeout::After(std::time::Duration::from_secs(1))) {
                Some(Ok(msg)) => return msg.payload().unwrap().into(),
                None => panic!("NONE"),
                Some(Err(x)) => panic!("{:?}", x),
            }
        }
    }
}

pub fn random_partition() -> i32 {
    thread_rng().gen_range(0..PARTITIONS)
}

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

pub struct Producer(OgProducer);

impl Deref for Producer {
    type Target = OgProducer;
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
        let mut client = OgProducer::from_hosts(bootstraps)
            .with_ack_timeout(Duration::from_millis(1000))
            .with_required_acks(RequiredAcks::All)
            .create()
            .unwrap();
        Self(client)
    }

    pub fn send(&mut self, topic: &str, partition: i32, item: &[u8]) -> (i32, i64) {
        let req = &[Record::from_value(topic, item)];
        let resp = self.0.send_all(req).unwrap();
        TOTAL_MB_WRITTEN.fetch_add(item.len(), SeqCst);
        let part = resp[0].partition_confirms[0].partition;
        let offset = resp[0].partition_confirms[0].offset.unwrap();
        //std::thread::sleep(std::time::Duration::from_secs(5));
        //let reqs = &[FetchPartition::new(topic, part, offset)
        //    .with_max_bytes(item.len().try_into().unwrap())];
        //let resps = self.0.fetch_messages(reqs).unwrap();
        //println!("here {}", resps.len());
        //println!("here {}", resps[0].topics().len());
        //println!("here {}", resps[0].topics()[0].partitions().len());
        //println!("here {}", resps[0].topics()[0].partitions()[0].data().unwrap().messages().len());
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
    pub fn new(bootstraps: &str, topic: &str, from: ConsumerOffset) -> Self {
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

pub enum ConsumerOffset {
    Latest,
    Earliest,
    From(SystemTime),
}

impl std::convert::Into<FetchOffset> for ConsumerOffset {
    fn into(self) -> FetchOffset {
        match self {
            ConsumerOffset::Latest => FetchOffset::Latest,
            ConsumerOffset::Earliest => FetchOffset::Earliest,
            ConsumerOffset::From(time) => {
                let time = time.duration_since(UNIX_EPOCH).unwrap();
                let millis = time.as_millis().try_into().unwrap();
                FetchOffset::ByTime(millis)
            }
        }
    }
}

fn init_consumer_worker(bootstraps: &str, topic: &str, data: InnerDict, from: ConsumerOffset) {

    let mut topic_partition_list = TopicPartitionList::new();
    for i in 0..PARTITIONS {
        topic_partition_list.add_partition(topic, i);
    }
    let consumer: BaseConsumer<DefaultConsumerContext> = ClientConfig::new()
        .set("bootstrap.servers", BOOTSTRAPS)
        .set("group.id", random_id())
        .create().unwrap();
    consumer.assign(&topic_partition_list).unwrap();
    std::thread::spawn(move || {
        loop {
            match consumer.poll(Timeout::After(std::time::Duration::from_secs(1))) {
                Some(Ok(msg)) => {
                    data.insert((msg.partition(), msg.offset()), msg.payload().unwrap().into());
                },
                Some(Err(x)) => panic!("{:?}", x),
                None => {}
            }
        }
    });

}
