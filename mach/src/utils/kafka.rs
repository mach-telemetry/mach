use crate::utils::random_id;
use kafka::client::{FetchPartition, KafkaClient, RequiredAcks};
use kafka::consumer::GroupOffsetStorage;
use kafka::producer::{Producer as OgProducer, Record};
use rand::{thread_rng, Rng};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
    //consumer::{BaseConsumer, Consumer as RdKConsumer, DefaultConsumerContext},
    //topic_partition_list::{Offset, TopicPartitionList},
    //util::Timeout,
    //Message,
};
//use std::convert::TryInto;
use crossbeam::channel::{unbounded, Receiver, Sender};
use dashmap::DashMap;
use lazy_static::lazy_static;
use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::Arc;
use std::time::Duration;

pub static TOTAL_MB_WRITTEN: AtomicUsize = AtomicUsize::new(0);

const PARTITIONS: i32 = 3;
const REPLICAS: i32 = 3;
pub const BOOTSTRAPS: &str = "localhost:9093,localhost:9094,localhost:9095";
pub const TOPIC: &str = "MACH";

pub const MAX_MSG_SZ: usize = 2_000_000;

lazy_static! {
    static ref KAFKA_CONSUMER: Arc<DashMap<(i32, i64), Arc<[u8]>>> = {
        use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
        let bootstraps = BOOTSTRAPS.split(',').map(String::from).collect();
        let mut consumer = Consumer::from_hosts(bootstraps)
            .with_topic(TOPIC.to_owned())
            .with_fallback_offset(FetchOffset::Latest)
            .with_group(random_id())
            .with_offset_storage(GroupOffsetStorage::Kafka)
            .with_fetch_max_bytes_per_partition(5_000_000)
            .create()
            .unwrap();
        let dict = Arc::new(DashMap::new());
        let dict2 = dict.clone();
        std::thread::spawn(move || loop {
            for ms in consumer.poll().unwrap().iter() {
                let partition = ms.partition();
                for m in ms.messages() {
                    let offset = m.offset;
                    let value = m.value;
                    dict2.insert((partition, offset), value.into());
                }
            }
        });
        dict
    };
    pub static ref PREFETCHER: Sender<KafkaEntry> = {
        let (tx, rx): (Sender<KafkaEntry>, Receiver<KafkaEntry>) = unbounded();
        //for _ in 0..4 {
        //    std::thread::spawn(move || {
        //        while let Ok(entry) = rx.recv() {
        //            entry.fetch();
        //        }
        //    });
        //}
        tx
    };

}

struct Cache {
    map: DashMap<(i32, i64), Arc<[u8]>>,
    size: AtomicUsize,
}

impl Cache {
    fn new() -> Self {
        Self {
            map: DashMap::new(),
            size: AtomicUsize::new(0),
        }
    }

    //fn fetch(&self, items: &[(i32, i64)])
}

pub fn init_kafka_consumer() {
    let _ = KAFKA_CONSUMER.clone();
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct KafkaEntry {
    items: Vec<(i32, i64)>,
}

impl KafkaEntry {
    pub fn new() -> Self {
        KafkaEntry { items: Vec::new() }
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    fn fetch(&self) {
        let mut hashset = HashSet::new();

        for item in self.items.iter().copied() {
            match KAFKA_CONSUMER.get(&item) {
                Some(kv) => {}
                None => {
                    hashset.insert(item);
                }
            }
        }

        let mut client = KafkaClient::new(BOOTSTRAPS.split(',').map(String::from).collect());
        client.load_metadata_all().unwrap();
        client.set_client_id(random_id());
        client.set_group_offset_storage(GroupOffsetStorage::Kafka);
        client.set_fetch_max_bytes_per_partition(5_000_000);
        client
            .set_fetch_max_wait_time(std::time::Duration::from_secs(1))
            .unwrap();
        client.set_fetch_min_bytes(0);

        //println!("loading from buffer at items {:?}", self.items);
        'poll_loop: loop {
            if hashset.len() == 0 {
                break;
            }
            for item in self.items.iter().copied() {
                if hashset.contains(&item) {
                    let reqs = &[FetchPartition::new(TOPIC, item.0, item.1)];
                    let resps = client.fetch_messages(reqs).unwrap();
                    for resp in resps {
                        for t in resp.topics() {
                            for p in t.partitions() {
                                match p.data() {
                                    Err(ref e) => {
                                        panic!(
                                            "partition error: {}:{}: {}",
                                            t.topic(),
                                            p.partition(),
                                            e
                                        )
                                    }
                                    Ok(ref data) => {
                                        for msg in data.messages() {
                                            if msg.value.len() > 0 {
                                                let key = (p.partition(), msg.offset);
                                                KAFKA_CONSUMER.insert(key, msg.value.into());
                                                hashset.remove(&key);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn load(&self, buffer: &mut Vec<u8>) -> Result<(), &'static str> {
        self.fetch();

        let mut hashset = HashSet::new();
        for item in self.items.iter() {
            for i in 1..10 {
                hashset.insert((item.0, item.1 - i as i64));
            }
            buffer.extend_from_slice(&KAFKA_CONSUMER.get(item).unwrap().value()[..]);
        }

        let mut items: Vec<(i32, i64)> = hashset.drain().collect();
        items.sort();
        let to_prefetch = Self { items };
        PREFETCHER.send(to_prefetch).unwrap();
        Ok(())
    }
}

//pub struct Client(KafkaClient);
//
//impl std::ops::Deref for Client {
//    type Target = KafkaClient;
//    fn deref(&self) -> &Self::Target {
//        &self.0
//    }
//}
//
//impl std::ops::DerefMut for Client {
//    fn deref_mut(&mut self) -> &mut Self::Target {
//        &mut self.0
//    }
//}
//
//impl Client {
//    pub fn new(bootstraps: &str) -> Self {
//        let bootstraps = bootstraps.split(',').map(String::from).collect();
//        let mut client = KafkaClient::new(bootstraps);
//        Self(client)
//    }
//
//    pub fn load(&mut self, topic: &str, partition: i32, offset: i64, max_bytes: usize) -> Arc<[u8]> {
//        let mut topic_partition_list = TopicPartitionList::new();
//        topic_partition_list.add_partition_offset(topic, partition, Offset::Offset(offset)).unwrap();
//        let consumer: BaseConsumer<DefaultConsumerContext> = ClientConfig::new()
//            .set("bootstrap.servers", BOOTSTRAPS)
//            .set("group.id", "random_consumer")
//            .create().unwrap();
//        consumer.assign(&topic_partition_list).unwrap();
//        loop {
//            match consumer.poll(Timeout::After(std::time::Duration::from_secs(1))) {
//                Some(Ok(msg)) => return msg.payload().unwrap().into(),
//                None => panic!("NONE"),
//                Some(Err(x)) => panic!("{:?}", x),
//            }
//        }
//    }
//}

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
    pub fn new() -> Self {
        let bootstraps = BOOTSTRAPS.split(',').map(String::from).collect();
        let client = OgProducer::from_hosts(bootstraps)
            .with_ack_timeout(Duration::from_millis(1000))
            .with_required_acks(RequiredAcks::All)
            .create()
            .unwrap();
        Self(client)
    }

    pub fn send(&mut self, item: &[u8]) -> KafkaEntry {
        let mut start = 0;

        let producer: &mut OgProducer = &mut self.0;

        let mut data = Vec::new();
        let mut rng = thread_rng();
        let mut items = Vec::new();
        while start < item.len() {
            let end = item.len().min(start + MAX_MSG_SZ);
            data.push(
                Record::from_value(TOPIC, &item[start..end])
                    .with_partition(rng.gen_range(0..PARTITIONS)),
            );
            let reqs = &[Record::from_value(TOPIC, &item[start..end])
                .with_partition(rng.gen_range(0..PARTITIONS))];
            let result = producer.send_all(reqs).unwrap();
            for topic in result.iter() {
                for partition in topic.partition_confirms.iter() {
                    let p = partition.partition;
                    let o = partition.offset.unwrap();
                    items.push((p, o))
                }
            }
            start = end;
        }
        //let result = producer.send_all(data.as_slice()).unwrap();
        //println!("Result Length: {:?}", result.len());
        //println!("Result: {:?}", result);
        //for topic in result.iter() {
        //    for partition in topic.partition_confirms.iter() {
        //        let p = partition.partition;
        //        let o = partition.offset.unwrap();
        //        items.push((p, o))
        //    }
        //}
        TOTAL_MB_WRITTEN.fetch_add(item.len(), SeqCst);
        let produced = KafkaEntry { items };
        produced
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use kafka::consumer::{Consumer, FetchOffset};
    use rand::{thread_rng, Fill, Rng};

    #[test]
    fn test_big() {
        let mut data = vec![0u8; 5_000_000];
        data.try_fill(&mut thread_rng()).unwrap();

        let mut producer = Producer::new();
        let key = producer.send(data.as_slice());
        std::thread::sleep(std::time::Duration::from_secs(5));

        println!("Key: {:?}", key);
        let mut result = Vec::new();
        let now = std::time::Instant::now();
        key.load(&mut result).unwrap();
        println!("time elapsed: {:?}", now.elapsed());
        println!("Result: {}", result.len());
        println!("Data: {}", data.len());
        assert_eq!(result, data);
    }
}
