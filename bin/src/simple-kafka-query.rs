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
use crate::completeness::{kafka::decompress_kafka_msg, Sample, SampleOwned, Writer, COUNTERS};

use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage, Message};
use lazy_static::*;
use mach::id::SeriesId;
use mach::utils::random_id;
use regex::Regex;
use std::collections::BTreeMap;
use std::ops::{Bound::Included, RangeBounds};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

enum Entry {
    Bytes(Arc<[u8]>),
    Processed(Vec<SampleOwned<SeriesId>>),
}

lazy_static! {
    static ref HOSTS: Vec<String> = vec!["localhost:9093".to_owned(),"localhost:9094".to_owned(),"localhost:9095".to_owned()];
    static ref SAMPLES: Arc<Mutex<Vec<Arc<[u8]>>>> = {
        let data = Arc::new(Mutex::new(Vec::new()));
        let data2 = data.clone();
        std::thread::spawn(move || {
            let mut consumer =
                Consumer::from_hosts(HOSTS.clone())
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
        let index = Arc::new(Mutex::new(BTreeMap::new()));
        let index2 = index.clone();
        std::thread::spawn(move || {
            let mut consumer =
                Consumer::from_hosts(HOSTS.clone())
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
                        let start = u64::from_be_bytes(m.value[0..8].try_into().unwrap());
                        let end = u64::from_be_bytes(m.value[8..16].try_into().unwrap());
                        assert!(index2.lock().unwrap().insert((start, end), Entry::Bytes(m.value.into())).is_none());
                        //data2.lock().unwrap().push(m.value.into());
                    }
                }
            }
        });
        index
    };
}

fn main() {
    println!("INITING SAMPLES HAS LEN: {}", INDEX.lock().unwrap().len());
    std::thread::sleep(std::time::Duration::from_secs(7 * 60));
    let id = SeriesId(20201894126936780);
    let now: u128 = micros_from_epoch();
    println!("NOW: {}", now);
    let last_5: u128 = now - Duration::from_secs(5 * 60).as_micros();
    let now: u64 = now.try_into().unwrap();
    let last_5: u64 = last_5.try_into().unwrap();

    // Waiting for timestamp
    println!("Waiting for timestamp {}", now);
    let start = Instant::now();
    let mut buf = vec![0u8; 500_000_000];
    let mut found = 0;
    'outer: loop {
        let guard = INDEX.lock().unwrap();
        let (s, e) = *guard.last_key_value().unwrap().0;
        if e > now {
            break 'outer;
        }
    }

    let wait = start.elapsed();
    println!("Searching backward");
    let mut counter = 0;
    let mut guard = INDEX.lock().unwrap();
    let low = Included((last_5, last_5));
    let high = Included((now, now));
    for (k, msg) in guard.range_mut((low, high)).rev() {
        let (s, e, samples) = match msg {
            Entry::Bytes(x) => decompress_kafka_msg(&x[..], &mut buf[..]),
            _ => panic!("unexpected"),
        };
        for item in samples.clone().iter().rev() {
            if item.0 == id && item.1 <= now && item.1 >= last_5 {
                //println!("{}, {}, {}", now, last_5, item.1);
                counter += 1;
            } else if item.1 < last_5 {
                break;
            }
        }
        *msg = Entry::Processed(samples);
    }

    let total = start.elapsed();
    let query = total - wait;
    println!("Counter: {}", counter);
    println!(
        "Total: {}, Delay: {}, Query: {}",
        total.as_secs_f64(),
        wait.as_secs_f64(),
        query.as_secs_f64()
    );

    println!("Querying again!");
    let mut counter = 0;
    let low = Included((last_5, last_5));
    let high = Included((now, now));
    let start = Instant::now();
    for (k, msg) in guard.range((low, high)).rev() {
        let samples: &[SampleOwned<SeriesId>] = match msg {
            Entry::Processed(x) => x.as_slice(),
            _ => panic!("unexpected"),
        };
        for item in samples.iter().rev() {
            if item.0 == id && item.1 <= now && item.1 >= last_5 {
                //println!("{}, {}, {}", now, last_5, item.1);
                counter += 1;
            } else if item.1 < last_5 {
                break;
            }
        }
    }
    println!("Counter: {}", counter);
    println!("Re-Query Time: {}", start.elapsed().as_secs_f64());
}

fn micros_from_epoch() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
}
