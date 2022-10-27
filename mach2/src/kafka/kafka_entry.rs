use crate::constants::*;
use kafka::{
    client::{fetch::Response, FetchPartition, KafkaClient},
    consumer::GroupOffsetStorage,
};
use rand::{
    distributions::{Alphanumeric, DistString},
    thread_rng,
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

fn new_client(size: i32) -> KafkaClient {
    let mut client = KafkaClient::new(BOOTSTRAPS.split(',').map(String::from).collect());
    client.load_metadata_all().unwrap();
    client.set_client_id(Alphanumeric.sample_string(&mut thread_rng(), 128));
    client.set_group_offset_storage(GroupOffsetStorage::Kafka);
    client.set_fetch_max_bytes_per_partition(size);
    client
        .set_fetch_max_wait_time(std::time::Duration::from_secs(1))
        .unwrap();
    client.set_fetch_min_bytes(0);
    client
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct KafkaEntry {
    items: Vec<(i32, i64)>,
}

impl From<Vec<(i32, i64)>> for KafkaEntry {
    fn from(items: Vec<(i32, i64)>) -> Self {
        Self { items }
    }
}

impl KafkaEntry {
    pub fn new() -> Self {
        KafkaEntry { items: Vec::new() }
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    fn fetch(&self) -> HashMap<(i32, i64), Arc<[u8]>> {
        //let mut tmp_block_store = THREAD_LOCAL_KAFKA_MESSAGES.borrow_mut();

        let mut hashmap = HashMap::new();
        let mut hashset = HashSet::new();
        for item in self.items.iter().copied() {
            hashset.insert(item);
        }

        let mut requests = Vec::new();
        let mut responses = Vec::new();
        let mut loop_counter = 0;
        'poll_loop: loop {
            let mut client = new_client(MAX_FETCH_BYTES);
            loop_counter += 1;
            if loop_counter > 0 && loop_counter % 1000 == 0 {
                println!("MIGHT HAVE ENTERED AN INFINITE LOOP HERE");
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
