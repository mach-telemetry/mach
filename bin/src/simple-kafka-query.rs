#![feature(map_first_last)]

#[allow(dead_code)]
mod bytes_server;
//mod completeness;
//mod elastic;
mod kafka_utils;

#[allow(dead_code)]
mod prep_data;
#[allow(dead_code)]
mod snapshotter;

#[allow(dead_code)]
mod utils;

#[allow(dead_code)]
mod batching;

#[allow(dead_code)]
mod constants;

//use crate::completeness::{kafka::decompress_kafka_msg, SampleOwned, Writer, COUNTERS};

use constants::*;
use dashmap::DashMap;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};//, Message};
use lazy_static::*;
use mach::id::SeriesId;
//use mach::utils::random_id;
use mach::sample::SampleType;
use rand::{self, prelude::*};
//use regex::Regex;
use std::collections::BTreeMap;
use std::ops::Bound::Included;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::thread;

use crossbeam::channel::{Sender, Receiver, bounded};

lazy_static! {
    static ref HOSTS: Vec<String> = {
        PARAMETERS.kafka_bootstraps.split(',').map(|x| x.into()).collect()
    };
    //static ref SAMPLES: Arc<Mutex<Vec<Arc<[u8]>>>> = {
    //    let data = Arc::new(Mutex::new(Vec::new()));
    //    let data2 = data.clone();
    //    std::thread::spawn(move || {
    //        let mut consumer = Consumer::from_hosts(HOSTS.clone())
    //            .with_topic_partitions("kafka-completeness-bench".to_owned(), &[0, 1, 2])
    //            .with_fallback_offset(FetchOffset::Latest)
    //            .with_group(random_id())
    //            .with_offset_storage(GroupOffsetStorage::Kafka)
    //            .with_fetch_max_bytes_per_partition(5_000_000)
    //            .create()
    //            .unwrap();
    //        //let mut counter = 0;
    //        loop {
    //            for ms in consumer.poll().unwrap().iter() {
    //                for m in ms.messages() {
    //                    data2.lock().unwrap().push(m.value.into());
    //                }
    //            }
    //        }
    //    });
    //    data
    //};
    static ref INDEX: Arc<Index> = Arc::new(Index::new());

    static ref SNAPSHOT_INTERVAL: Duration = Duration::from_secs_f64(0.5);
    static ref SNAPSHOT_TIMEOUT: Duration = Duration::from_secs_f64(0.5);
}


struct Index {
    index: Arc<DashMap<SeriesId, Arc<Mutex<BTreeMap<(u64, u64), Entry>>>>>,
    out_queue: Receiver<(SeriesId, u64, u64)>,
    in_queue: Sender<(SeriesId, u64, u64)>,
}

impl Index {
    fn insert(&self, id: SeriesId, range: (u64, u64), entry: Entry) {
        if self.in_queue.is_full() {
            let x = self.out_queue.try_recv().unwrap();
            self.index.get(&x.0).unwrap().lock().unwrap().remove(&(x.1, x.2)).unwrap();
        }

        self.index
            .entry(id)
            .or_insert(Arc::new(Mutex::new(BTreeMap::new())))
            .value()
            .lock()
            .unwrap()
            .insert(range, entry);
        self.in_queue.send((id, range.0, range.1)).unwrap();
    }

    //fn last_timestamp(&self, id: SeriesId) -> Option<(u64, u64)> {
    //    Some(*self.index.get(&id)?.lock().unwrap().last_key_value()?.0)
    //}

    fn get_map(&self, id: SeriesId) -> Option<Arc<Mutex<BTreeMap<(u64, u64), Entry>>>> {
        Some(self.index.get(&id)?.clone())
    }

    fn new() -> Self {
        let (in_queue, out_queue) = bounded(PARAMETERS.kafka_index_size);
        Index {
            index: Arc::new(DashMap::new()),
            in_queue,
            out_queue,
        }
    }
}

#[derive(Clone)]
enum Entry {
    Bytes(batching::BytesBatch),
    Processed(Arc<Vec<(u64, u64, Vec<SampleType>)>>),
}

fn consumer() {
    let index = INDEX.clone();
    let mut consumer = Consumer::from_hosts(HOSTS.clone())
        .with_topic(PARAMETERS.kafka_topic.clone())
        .with_fallback_offset(FetchOffset::Latest)
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .with_fetch_max_bytes_per_partition(PARAMETERS.kafka_batch_bytes as i32) // messages are actually MUCH smaller than batch size due to compression
        .create()
        .unwrap();

    loop {
        for ms in consumer.poll().unwrap().iter() {
            for m in ms.messages() {
                let batch = batching::BytesBatch::new(m.value.into());
                let (ids, (low, high)) = batch.metadata();
                let entry = Entry::Bytes(batch);
                for id in ids.iter() {
                    index.insert(SeriesId(*id), (low, high), entry.clone());
                }
            }
        }
    }
}

fn init_consumer() {
    thread::spawn(consumer);
}

fn execute_query(i: usize, source: SeriesId, start: u64, end: u64) {
    // Waiting for timestamp
    //println!("Waiting for timestamp {}", now);
    //let mut buf = vec![0u8; 500_000_000];

    let timer = Instant::now();
    let index = INDEX.get_map(source).unwrap();

    'outer: loop {
        let guard = index.lock().unwrap();
        let (_s, e) = *guard.last_key_value().unwrap().0;
        if e > start {
            break 'outer;
        }
        drop(guard);
        let now = Instant::now();
        while now.elapsed() < Duration::from_millis(100) {}
    }
    let data_latency = timer.elapsed();

    let mut counter = 0;
    let mut guard = index.lock().unwrap();
    let timer = Instant::now();
    let low = Included((end, end));
    let high = Included((start, start));
    for (_k, msg) in guard.range_mut((low, high)).rev() {
        let samples = match msg {
            Entry::Bytes(x) => {
                let entries = x.entries();
                *msg = Entry::Processed(Arc::new(entries));
                match msg {
                    Entry::Processed(samples) => samples.clone(),
                    _ => unreachable!(),
                }
            }
            Entry::Processed(x) => x.clone(),
        };
        for item in samples.iter().rev() {
            if item.0 == source.0 && item.1 <= start && item.1 >= end {
                counter += 1;
            } else if item.1 < end {
                break;
            }
        }
    }
    let total_latency = timer.elapsed();
    print!("Query ID: {}, ", i);
    print!("Total Latency: {}, ", total_latency.as_secs_f64());
    print!("Data Latency: {}, ", data_latency.as_secs_f64());
    print!("Execution Latency: {}, ", (total_latency - data_latency).as_secs_f64());
    print!("Sink: {}, ", counter);
    println!("");
}

//const START_MAX_DELAY: u64 = 60;
//const MIN_QUERY_DURATION: u64 = 10;
//const MAX_QUERY_DURATION: u64 = 60;
//const SOURCE_COUNT: u64 = 1000;
//const QUERY_COUNT: u64 = 1;

fn main() {
    init_consumer();

    println!("Sleeping");
    thread::sleep(Duration::from_secs(2 * PARAMETERS.query_max_delay));
    println!("Done Sleeping");

    let mut rng = rand::thread_rng();
    let micros_in_sec: u64 = Duration::from_secs(1).as_micros().try_into().unwrap();

    for i in 0..PARAMETERS.query_count as usize {
        thread::sleep(Duration::from_secs(10));
        let id = SeriesId(rng.gen_range(0..PARAMETERS.source_count));
        let now: u64 = micros_from_epoch().try_into().unwrap();
        let start = now - rng.gen_range(0..PARAMETERS.query_max_delay) * micros_in_sec;
        let end = start - rng.gen_range(PARAMETERS.min_query_duration..PARAMETERS.max_query_duration) * micros_in_sec;

        thread::spawn(move || {
            execute_query(i, id, start, end);
        });
        // todo: Fire off query
    }
}

fn micros_from_epoch() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
}
