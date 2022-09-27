#![feature(map_first_last)]

#[allow(dead_code)]
mod bytes_server;
mod completeness;
mod elastic;
mod kafka_utils;
#[allow(dead_code)]
mod prep_data;
#[allow(dead_code)]
mod snapshotter;
mod utils;
use crate::completeness::{kafka::decompress_kafka_msg, SampleOwned, Writer, COUNTERS};

use dashmap::DashMap;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage, Message};
use lazy_static::*;
use mach::id::SeriesId;
use mach::utils::random_id;
use rand::{self, prelude::*};
use regex::Regex;
use std::collections::BTreeMap;
use std::ops::{Bound::Included, RangeBounds};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

struct Index(Arc<DashMap<SeriesId, Arc<Mutex<BTreeMap<(u64, u64), Arc<Entry>>>>>>);

impl Index {
    fn insert(&self, id: SeriesId, range: (u64, u64), entry: Arc<Entry>) {
        self.0
            .entry(id)
            .or_insert(Arc::new(Mutex::new(BTreeMap::new())))
            .value()
            .lock()
            .unwrap()
            .insert(range, entry);
    }

    fn last_timestamp(&self, id: SeriesId) -> Option<(u64, u64)> {
        Some(*self.0.get(&id)?.lock().unwrap().last_key_value()?.0)
    }

    fn get_map(&self, id: SeriesId) -> Option<Arc<Mutex<BTreeMap<(u64, u64), Arc<Entry>>>>> {
        Some(self.0.get(&id)?.clone())
    }
}

enum Entry {
    Bytes(Box<[u8]>),
    Processed(Vec<SampleOwned<SeriesId>>),
}

lazy_static! {
    static ref HOSTS: Vec<String> = vec![
        "localhost:9093".to_owned(),
        "localhost:9094".to_owned(),
        "localhost:9095".to_owned()
    ];
    static ref SAMPLES: Arc<Mutex<Vec<Arc<[u8]>>>> = {
        let data = Arc::new(Mutex::new(Vec::new()));
        let data2 = data.clone();
        std::thread::spawn(move || {
            let mut consumer = Consumer::from_hosts(HOSTS.clone())
                .with_topic_partitions("kafka-completeness-bench".to_owned(), &[0, 1, 2])
                .with_fallback_offset(FetchOffset::Latest)
                .with_group(random_id())
                .with_offset_storage(GroupOffsetStorage::Kafka)
                .with_fetch_max_bytes_per_partition(5_000_000)
                .create()
                .unwrap();
            let mut counter = 0;
            loop {
                for ms in consumer.poll().unwrap().iter() {
                    for m in ms.messages() {
                        data2.lock().unwrap().push(m.value.into());
                    }
                }
            }
        });
        data
    };
    static ref INDEX: Arc<Mutex<BTreeMap<(u64, u64), Entry>>> = {
        println!("INITING INDEX");
        let index: Arc<Mutex<BTreeMap<(u64, u64), Entry>>> = Arc::new(Mutex::new(BTreeMap::new()));
        let group = random_id();
        let index2 = index.clone();
        let micros_in_sec: u64 = Duration::from_secs(1).as_micros().try_into().unwrap();
        for i in 0..3 {
            let index2 = index.clone();
            let group = group.clone();
            std::thread::spawn(move || {
                let mut consumer = Consumer::from_hosts(HOSTS.clone())
                    .with_topic_partitions("kafka-completeness-bench".to_owned(), &[i])
                    .with_fallback_offset(FetchOffset::Latest)
                    .with_offset_storage(GroupOffsetStorage::Kafka)
                    .with_fetch_max_bytes_per_partition(5_000_000)
                    .create()
                    .unwrap();
                let mut counter = 0;
                loop {
                    for ms in consumer.poll().unwrap().iter() {
                        for m in ms.messages() {
                            let start = u64::from_be_bytes(m.value[0..8].try_into().unwrap());
                            let end = u64::from_be_bytes(m.value[8..16].try_into().unwrap());
                            let mut guard = index2.lock().unwrap();
                            let key = (start, end);
                            let value = Entry::Bytes(m.value.into());
                            assert!(guard.insert(key, value).is_none());
                            let now: u64 = micros_from_epoch().try_into().unwrap();
                            let start = now - 2 * START_MAX_DELAY * micros_in_sec;
                            match guard.first_key_value() {
                                Some(x) => {
                                    let key = *x.0;
                                    assert!(key.0 < key.1);
                                    if key.1 < start {
                                        drop(x);
                                        guard.remove(&key).unwrap();
                                    }
                                }
                                None => {}
                            }
                        }
                    }
                }
            });
        }
        index
    };
    static ref SNAPSHOT_INTERVAL: Duration = Duration::from_secs_f64(0.5);
    static ref SNAPSHOT_TIMEOUT: Duration = Duration::from_secs_f64(0.5);
}

const START_MAX_DELAY: u64 = 60;
const MIN_QUERY_DURATION: u64 = 10;
const MAX_QUERY_DURATION: u64 = 60;
const SOURCE_COUNT: u64 = 1000;
const QUERY_COUNT: u64 = 1;

fn main() {
    {
        let _ = INDEX.lock().unwrap();
    }

    println!("Sleeping");
    std::thread::sleep(Duration::from_secs(2 * START_MAX_DELAY));
    println!("Done Sleeping");

    let mut rng = rand::thread_rng();
    let micros_in_sec: u64 = Duration::from_secs(1).as_micros().try_into().unwrap();

    for _ in 0..QUERY_COUNT {
        let id = SeriesId(rng.gen_range(0..SOURCE_COUNT));
        let now: u64 = micros_from_epoch().try_into().unwrap();
        let start = now - rng.gen_range(0..START_MAX_DELAY) * micros_in_sec;
        let end = start - rng.gen_range(MIN_QUERY_DURATION..MAX_QUERY_DURATION) * micros_in_sec;
        // Waiting for timestamp
        //println!("Waiting for timestamp {}", now);
        let mut buf = vec![0u8; 500_000_000];
        let mut found = 0;

        let timer = Instant::now();
        'outer: loop {
            let guard = INDEX.lock().unwrap();
            let (s, e) = *guard.last_key_value().unwrap().0;
            //println!("{} {} {}", s, e, start);
            if e > start {
                break 'outer;
            }
            drop(guard);
            let now = Instant::now();
            while now.elapsed() < Duration::from_millis(100) {}
        }
        let data_latency = timer.elapsed();
        //println!("Searching backward");
        let mut counter = 0;
        let mut guard = INDEX.lock().unwrap();
        let timer = Instant::now();
        let low = Included((end, end));
        let high = Included((start, start));
        for (k, msg) in guard.range_mut((low, high)).rev() {
            let samples = match msg {
                Entry::Bytes(x) => {
                    let (s, e, samples) = decompress_kafka_msg(&x[..], &mut buf[..]);
                    *msg = Entry::Processed(samples);
                    match msg {
                        Entry::Processed(samples) => samples.as_slice(),
                        _ => unreachable!(),
                    }
                }
                Entry::Processed(x) => x,
            };
            for item in samples.iter().rev() {
                if item.0 == id && item.1 <= start && item.1 >= end {
                    //println!("{}, {}, {}", now, last_5, item.1);
                    counter += 1;
                } else if item.1 < end {
                    break;
                }
            }
        }

        let query_latency = timer.elapsed();
        let total_latency = query_latency + data_latency;
        println!(
            "Total Latency: {}, Data Latency: {}, Query Latency: {}",
            total_latency.as_secs_f64(),
            data_latency.as_secs_f64(),
            query_latency.as_secs_f64()
        );
    }
}

fn micros_from_epoch() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
}
