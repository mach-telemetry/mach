#![feature(map_first_last)]

mod query_utils;

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

#[allow(dead_code)]
mod data_generator;

//use crate::completeness::{kafka::decompress_kafka_msg, SampleOwned, Writer, COUNTERS};

use constants::*;
use dashmap::DashMap;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage}; //, Message};
use lazy_static::*;
use mach;
use mach::id::SeriesId;
//use mach::utils::random_id;
use mach::sample::SampleType;
//use rand::{self, prelude::*};
//use regex::Regex;
use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::collections::BTreeMap;
use std::ops::Bound::Included;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use utils::NotificationReceiver;

use crossbeam::channel::{bounded, unbounded, Receiver, Sender};
use query_utils::SimpleQuery;

lazy_static! {
    static ref HOSTS: Vec<String> = {
        PARAMETERS
            .kafka_bootstraps
            .split(',')
            .map(|x| x.into())
            .collect()
    };
    static ref INDEX: Arc<Index> = {
        let meta_index = Arc::new(Index::new());
        for id in 0..PARAMETERS.source_count {
            meta_index
                .index
                .insert(SeriesId(id), Arc::new(Mutex::new(BTreeMap::new())));
        }
        meta_index
    };
}

struct Index {
    index: Arc<DashMap<SeriesId, Arc<Mutex<BTreeMap<(u64, u64), Entry>>>>>,
    out_queue: Receiver<(SeriesId, u64, u64)>,
    in_queue: Sender<(SeriesId, u64, u64)>,
}

impl Index {
    fn insert(&self, id: SeriesId, range: (u64, u64), entry: Entry) {
        //if self.in_queue.is_full() {
        //    let x = self.out_queue.try_recv().unwrap();
        //    self.index.get(&x.0).unwrap().lock().unwrap().remove(&(x.1, x.2)).unwrap();
        //}

        self.index
            .entry(id)
            .or_insert(Arc::new(Mutex::new(BTreeMap::new())))
            .value()
            .lock()
            .unwrap()
            .insert(range, entry);
        //self.in_queue.send((id, range.0, range.1)).unwrap();
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
        .with_fetch_max_bytes_per_partition((PARAMETERS.kafka_batch_bytes * 2) as i32) // messages are actually MUCH smaller than batch size due to compression
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

fn execute_query(i: usize, query: SimpleQuery, signal: Sender<()>) {
    println!("Executing query: {}", i);
    // Waiting for timestamp
    //println!("Waiting for timestamp {}", now);
    //let mut buf = vec![0u8; 500_000_000];

    if *query.source == 385 {
        println!("Query start: {}", query.start);
    }

    let source = query.source;
    let start = query.start;
    let end = query.end;

    let timer = Instant::now();
    let index = INDEX.get_map(source).unwrap();

    let now = chrono::prelude::Utc::now();
    'outer: loop {
        let guard = index.lock().unwrap();
        if guard.len() == 0 {
            continue;
        }
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
    let mut blocks_seen = 0;
    let mut guard = index.lock().unwrap();
    //let timer = Instant::now();
    let low = Included((end, end));
    let high = Included((start, start));
    for (_k, msg) in guard.range_mut((low, high)).rev() {
        blocks_seen += 1;
        let samples = match msg {
            Entry::Bytes(x) => {
                let entries = x.entries();
                entries.clone()
            }
            Entry::Processed(x) => unimplemented!(),
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
    let execution_latency = total_latency - data_latency;
    print!("Current time: {:?}, ", now);
    print!("Query ID: {}, ", i);
    print!("Source: {:?}, ", source);
    print!("Duration: {}, ", start - end);
    print!("From now: {}, ", query.from_now);
    print!("Total Latency: {}, ", total_latency.as_secs_f64());
    print!("Data Latency: {}, ", data_latency.as_secs_f64());
    print!("Execution Latency: {}, ", execution_latency.as_secs_f64());
    print!("Sink: {}, ", counter);
    print!("Blocks seen: {}, ", blocks_seen);
    println!("");
    signal.send(()).unwrap();
}

//const START_MAX_DELAY: u64 = 60;
//const MIN_QUERY_DURATION: u64 = 10;
//const MAX_QUERY_DURATION: u64 = 60;
//const SOURCE_COUNT: u64 = 1000;
//const QUERY_COUNT: u64 = 1;

fn main() {
    init_consumer();

    let mut start_notif = NotificationReceiver::new(PARAMETERS.querier_port);
    println!("Waiting for workload to start...");
    start_notif.wait();
    println!("Workload started");

    let mut rng = ChaCha8Rng::seed_from_u64(PARAMETERS.query_rand_seed);
    let num_sources: usize = (PARAMETERS.source_count / 10).try_into().unwrap();
    let sources = data_generator::HOT_SOURCES.as_slice();

    // Sleeping to make sure there's enough data
    let initial_sleep_secs = 2 * PARAMETERS.query_max_delay;
    println!("Sleep for {initial_sleep_secs} seconds to wait for data arrival");
    std::thread::sleep(Duration::from_secs(initial_sleep_secs));
    println!("Done Sleeping");

    let (tx, rx) = unbounded();

    for i in 0..(PARAMETERS.query_count as usize) {
        thread::sleep(Duration::from_secs(PARAMETERS.query_interval_seconds));
        let now: u64 = utils::timestamp_now_micros().try_into().unwrap();
        let query = {
            let mut q = SimpleQuery::new_relative_to(now);
            let source_idx = rng.gen_range(0..sources.len());
            q.source = sources[source_idx];
            q
        };

        let tx = tx.clone();
        thread::spawn(move || {
            execute_query(i, query, tx);
        });
        // todo: Fire off query
    }
    drop(tx);
    while let Ok(_) = rx.recv() {}
}
