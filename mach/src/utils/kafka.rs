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
use std::collections::{HashSet, HashMap};
use rand::{Rng, thread_rng};

pub static TOTAL_MB_WRITTEN: AtomicUsize = AtomicUsize::new(0);

const PARTITIONS: i32 = 3;
const REPLICAS: i32 = 3;
pub const BOOTSTRAPS: &str = "localhost:9093,localhost:9094,localhost:9095";
pub const TOPIC: &str = "MACH";

pub const MAX_MSG_SZ: usize = 2_000_000;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct KafkaEntry {
    items: Vec<(i32, i64)>
}

impl KafkaEntry {
    pub fn new() -> Self {
        KafkaEntry {
            items: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn load(&self, buffer: &mut Vec<u8>) -> Result<usize, &'static str> {

        let mut hashset = HashSet::new();
        //let mut hashmap = HashMap::new();

        for item in self.items.iter().copied() {
            hashset.insert(item);
            //reqs.push(FetchPartition::new(TOPIC, item.0, item.1).with_max_bytes(MAX_MSG_SZ.try_into().unwrap()));
        }

        let mut client = KafkaClient::new(BOOTSTRAPS.split(',').map(String::from).collect());
        client.load_metadata_all().unwrap();
        client.set_client_id(random_id());
        client.set_group_offset_storage(GroupOffsetStorage::Kafka);
        client.set_fetch_max_bytes_per_partition(5_000_000);
        client.set_fetch_max_wait_time(std::time::Duration::from_secs(1)).unwrap();
        client.set_fetch_min_bytes(0);

        let mut hashmap: HashMap<(i32, i64), Vec<u8>> = HashMap::new();
        'poll_loop: loop {
            for item in self.items.iter().copied() {
                if hashset.contains(&item) {
                    let reqs = &[FetchPartition::new(TOPIC, item.0, item.1)];
                    let resps = client.fetch_messages(reqs).unwrap();
                    for resp in resps {
                        for t in resp.topics() {
                            for p in t.partitions() {
                                match p.data() {
                                    Err(ref e) => {
                                        panic!("partition error: {}:{}: {}", t.topic(), p.partition(), e)
                                    }
                                    Ok(ref data) => {
                                        for msg in data.messages() {
                                            if msg.value.len() > 0 {
                                                let key = (p.partition(), msg.offset);
                                                if hashset.contains(&key) {
                                                    hashmap.insert(key, msg.value.into());
                                                    hashset.remove(&(p.partition(), msg.offset));
                                                }
                                                if hashset.is_empty() {
                                                    break 'poll_loop;
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

        for item in self.items.iter() {
            buffer.extend_from_slice(hashmap.get(item).unwrap());
        }
        Ok(0)
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
        let mut client = OgProducer::from_hosts(bootstraps)
            .with_ack_timeout(Duration::from_millis(1000))
            .with_required_acks(RequiredAcks::All)
            .create()
            .unwrap();
        Self(client)
    }

    pub fn send(&mut self, item: &[u8]) -> KafkaEntry {
        let mut start = 0;
        let mut end = MAX_MSG_SZ;

        let producer: &mut OgProducer = &mut self.0;

        let mut data = Vec::new();
        let mut rng = thread_rng();
        let mut items = Vec::new();
        while start < item.len() {
            let end = item.len().min(start + MAX_MSG_SZ);
            data.push(Record::from_value(TOPIC, &item[start..end]).with_partition(rng.gen_range(0..PARTITIONS)));
            let reqs = &[Record::from_value(TOPIC, &item[start..end]).with_partition(rng.gen_range(0..PARTITIONS))];
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
        let produced = KafkaEntry {
            items,
        };
        produced
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::{Rng, Fill, thread_rng};
    use kafka::consumer::{Consumer, FetchOffset};


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

