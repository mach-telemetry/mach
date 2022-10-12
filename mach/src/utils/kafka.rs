use crate::utils::random_id;
use kafka::client::{fetch::Response, FetchPartition, KafkaClient, RequiredAcks};
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
use crate::constants::*;
use crate::utils::counter::*;
use crate::utils::timer::*;
use ref_thread_local::{ref_thread_local, RefThreadLocal};
use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::Arc;
use std::time::Duration;
use lazy_static::*;
use std::thread;
use num::NumCast;

ref_thread_local! {
    static managed THREAD_LOCAL_CONSUMER: KafkaClient = {
        let mut client = KafkaClient::new(BOOTSTRAPS.split(',').map(String::from).collect());
        client.load_metadata_all().unwrap();
        client.set_client_id(random_id());
        client.set_group_offset_storage(GroupOffsetStorage::Kafka);
        client.set_fetch_max_bytes_per_partition(2_000_000);
        client
            .set_fetch_max_wait_time(std::time::Duration::from_secs(1))
            .unwrap();
        client.set_fetch_min_bytes(0);
        client
    };

    static managed THREAD_LOCAL_KAFKA_MESSAGES: HashMap<(i32, i64), Arc<[u8]>> = HashMap::new();
}

lazy_static! {
    static ref TOTAL_MB_WRITTEN: Arc<AtomicUsize> = {
        let x = Arc::new(AtomicUsize::new(0));
        let x2 = x.clone();
        thread::spawn(move || {
            let mut last = 0.;
            loop {
                let x = x2.load(SeqCst);
                let x2 = <f64 as NumCast>::from(x).unwrap();
                let mb = (x2 - last) / 1_000_000.;
                println!("Total mb written to Kafka: {} mbps, {} total mb", mb, x);
                last = x2;
                thread::sleep(Duration::from_secs(1));
            }
        });
        x
    };
}

fn parse_response(resps: Vec<Response>, buffer: &mut Vec<(i32, i64, Arc<[u8]>)>) {
    buffer.clear();
    for resp in resps {
        for t in resp.topics() {
            for p in t.partitions() {
                match p.data() {
                    Err(ref e) => {
                        panic!("partition error: {}:{}: {}", t.topic(), p.partition(), e)
                    }
                    Ok(data) => {
                        for msg in data.messages() {
                            if !msg.value.is_empty() {
                                buffer.push((p.partition(), msg.offset, msg.value.into()));
                            }
                        }
                    }
                }
            }
        }
    }
}

fn make_requests(set: &HashSet<(i32, i64)>, buffer: &mut Vec<FetchPartition>) {
    buffer.clear();
    for item in set {
        buffer.push(FetchPartition::new(TOPIC, item.0, item.1));
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct KafkaEntry {
    items: Vec<(i32, i64)>,
}

fn new_client(size: i32) -> KafkaClient {
    let mut client = KafkaClient::new(BOOTSTRAPS.split(',').map(String::from).collect());
    client.load_metadata_all().unwrap();
    client.set_client_id(random_id());
    client.set_group_offset_storage(GroupOffsetStorage::Kafka);
    client.set_fetch_max_bytes_per_partition(size);
    client
        .set_fetch_max_wait_time(std::time::Duration::from_secs(1))
        .unwrap();
    client.set_fetch_min_bytes(0);
    client
}

impl KafkaEntry {
    pub fn new() -> Self {
        KafkaEntry { items: Vec::new() }
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    fn fetch(&self) -> HashMap<(i32, i64), Arc<[u8]>> {
        let _timer_1 = ThreadLocalTimer::new("KafkaEntry::fetch");
        ThreadLocalCounter::new("kafka fetch").increment(1);

        //let mut tmp_block_store = THREAD_LOCAL_KAFKA_MESSAGES.borrow_mut();

        let mut hashmap = HashMap::new();
        let mut hashset = HashSet::new();
        for item in self.items.iter().copied() {
            //if let Some(block) = tmp_block_store.get(&item) {
            //    hashmap.insert(item, block.clone());
            //    ThreadLocalCounter::new("kafka message already fetched").increment(1);
            //} else {
                hashset.insert(item);
            //}
        }

        let mut requests = Vec::new();
        let mut responses = Vec::new();
        //let mut client = THREAD_LOCAL_CONSUMER.borrow_mut();
        let mut loop_counter = 0;
        'poll_loop: loop {
            let mut client = new_client(MAX_FETCH_BYTES);
            loop_counter += 1;
            if loop_counter > 0 && loop_counter % 1000 == 0 {
                println!("HERE");
                client = new_client(MAX_FETCH_BYTES * 2);
            }
            if hashset.is_empty() {
                break 'poll_loop;
            }
            make_requests(&hashset, &mut requests);
            parse_response(
                client.fetch_messages(requests.as_slice()).unwrap(),
                &mut responses,
            );
            for (p, o, bytes) in responses.drain(..) {
                let key = (p, o);
                hashmap.insert(key, bytes.clone());
                //tmp_block_store.insert(key, bytes);
                hashset.remove(&key);
            }
        }
        ThreadLocalCounter::new("kafka messages fetched").increment(hashmap.len());
        hashmap
    }

    pub fn load(&self, buffer: &mut Vec<u8>) -> Result<(), &'static str> {
        let blocks = self.fetch();
        for item in self.items.iter() {
            buffer.extend_from_slice(&blocks.get(item).unwrap()[..]);
        }
        Ok(())
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
    pub fn new() -> Self {
        let bootstraps = BOOTSTRAPS.split(',').map(String::from).collect();
        let client = OgProducer::from_hosts(bootstraps)
            .with_ack_timeout(Duration::from_millis(3000))
            .with_required_acks(RequiredAcks::All)
            .create()
            .unwrap();
        Self(client)
    }

    pub fn send(&mut self, item: &[u8]) -> KafkaEntry {
        //return KafkaEntry::new();
        //println!("item length: {}", item.len());
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
        ////println!("Result Length: {:?}", result.len());
        ////println!("Result: {:?}", result);
        //for topic in result.iter() {
        //    for partition in topic.partition_confirms.iter() {
        //        let p = partition.partition;
        //        let o = partition.offset.unwrap();
        //        items.push((p, o))
        //    }
        //}
        TOTAL_MB_WRITTEN.fetch_add(item.len(), SeqCst);
        KafkaEntry { items }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::{thread_rng, Fill};

    #[test]
    fn test_big() {
        let mut data = vec![0u8; 5_000_000];
        data.try_fill(&mut thread_rng()).unwrap();

        let mut producer = Producer::new();
        let key = producer.send(data.as_slice(), 0..PARTITIONS);
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
