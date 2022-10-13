#[allow(dead_code)]
mod bytes_server;
#[allow(dead_code)]
mod snapshotter;

#[allow(dead_code)]
mod constants;

mod query_utils;

#[allow(dead_code)]
mod utils;

#[allow(dead_code)]
mod data_generator;

use dashmap::DashMap;
use mach;
use mach::id::SeriesId;
use mach::snapshotter::SnapshotterId;
use mach::utils::counter::*;
use mach::utils::timer::*;
use std::sync::Arc;
//use mach::utils::kafka::init_thread_local_consumer;
use rand::prelude::*;
use regex::Regex;
use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use constants::*;
use crossbeam::channel::{unbounded, Receiver, Sender};
use query_utils::SimpleQuery;
use std::thread;
use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use utils::NotificationReceiver;

lazy_static::lazy_static! {
    static ref SNAPSHOT_INTERVAL: Duration = Duration::from_secs_f64(PARAMETERS.mach_snapshot_interval);
    static ref SNAPSHOT_TIMEOUT: Duration = Duration::from_secs_f64(PARAMETERS.mach_snapshot_timeout);
    static ref SNAPSHOTTER_MAP: Arc<DashMap<SeriesId, SnapshotterId>> = Arc::new(DashMap::new());
}

//const START_MAX_DELAY: u64 = 60;
//const MIN_QUERY_DURATION: u64 = 10;
//const MAX_QUERY_DURATION: u64 = 60;
//const SOURCE_COUNT: u64 = 1000;
//const QUERY_COUNT: u64 = 100;

fn execute_query(i: usize, query: SimpleQuery, done_notifier: Sender<()>) {
    println!("Executing query: {}", i);
    let source = query.source;
    let start = query.start;
    let end = query.end;
    let interval = *SNAPSHOT_INTERVAL;
    let timeout = *SNAPSHOT_TIMEOUT;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut client = runtime.block_on(snapshotter::SnapshotClient::new(PARAMETERS.snapshot_server_port.as_str()));

    let snapshotter_id = *SNAPSHOTTER_MAP
        .entry(source)
        .or_insert(
            runtime
                .block_on(client.initialize(source, interval, timeout))
                .unwrap(),
        )
        .value();

    // Wait for timestamp to be available
    let now = chrono::prelude::Utc::now();
    let timer = Instant::now();
    loop {
        let now = Instant::now();
        let snapshot_id = runtime.block_on(client.get(snapshotter_id)).unwrap();
        let mut snapshot = snapshot_id.load().into_iterator();
        snapshot.next_segment().unwrap();
        let seg = snapshot.get_segment();
        let mut timestamps = seg.timestamps().iterator();
        let ts = timestamps.next().unwrap();
        if ts > start {
            break;
        }
        while now.elapsed() < interval {}
    }

    let data_latency = timer.elapsed();

    //println!("Executing query");
    //ThreadLocalTimer::reset();
    //ThreadLocalCounter::reset();
    let result_count = {
        //let _timer_1 = ThreadLocalTimer::new("query execution");
        let snapshot_id = runtime.block_on(client.get(snapshotter_id)).unwrap();
        let mut snapshot = snapshot_id.load().into_iterator();
        //println!("Range: {} {}", start, end);
        let mut count = 0;
        'outer: loop {
            if snapshot.next_segment_at_timestamp(start).is_none() {
                break;
            }
            let seg = snapshot.get_segment();
            let mut timestamps = seg.timestamps().iterator();
            for (i, ts) in timestamps.enumerate() {
                count += 1;
                if ts < end {
                    break 'outer;
                }
            }
        }
        count
    };
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
    print!("Sink: {}, ", result_count);
    println!("");

    done_notifier.send(()).unwrap();
}

fn main() {

    let mut rng = ChaCha8Rng::seed_from_u64(PARAMETERS.query_rand_seed);
    let num_sources: usize = (PARAMETERS.source_count / 10).try_into().unwrap();
    let sources = data_generator::HOT_SOURCES.as_slice();

    let mut start_notif = NotificationReceiver::new(PARAMETERS.querier_port);
    println!("Waiting for workload to start...");
    start_notif.wait();
    println!("Workload started");

    // Sleeping to make sure there's enough data
    let initial_sleep_secs = 2 * PARAMETERS.query_max_delay;
    println!("Sleep for {initial_sleep_secs} seconds to wait for data arrival");
    std::thread::sleep(Duration::from_secs(initial_sleep_secs));
    println!("Done Sleeping");

    let (notification_channel, wait_notification_channel) = unbounded();
    for i in 0..(PARAMETERS.query_count as usize) {
        thread::sleep(Duration::from_secs(PARAMETERS.query_interval_seconds));
        let now: u64 = utils::timestamp_now_micros().try_into().unwrap();
        let query = {
            let mut q = SimpleQuery::new_relative_to(now);
            let source_idx = rng.gen_range(0..sources.len());
            q.source = sources[source_idx];
            q
        };

        let tx = notification_channel.clone();
        thread::spawn(move || {
            execute_query(i, query, tx);
        });
    }

    drop(notification_channel);
    while let Ok(_) = wait_notification_channel.recv() {}
}

//let id = SeriesId(rng.gen_range(0..SOURCE_COUNT));
//let snapshotter_id = *snapshotter_map.entry(id).or_insert(
//    runtime
//        .block_on(client.initialize(id, interval, timeout))
//        .unwrap(),
//);
//let now: u64 = micros_from_epoch().try_into().unwrap();
//let start_delay = rng.gen_range(0..START_MAX_DELAY);
//let to_end = rng.gen_range(MIN_QUERY_DURATION..MAX_QUERY_DURATION);
//let start = now - start_delay * micros_in_sec;
//let end = start - to_end * micros_in_sec;

//let mut count = 0;

//let timer = Instant::now();
//let mut query_execution_time = Duration::from_secs(0);
////println!("Waiting for data to be available");
//loop {
//    let snapshot_id = runtime.block_on(client.get(snapshotter_id)).unwrap();
//    let mut snapshot = snapshot_id.load().into_iterator();
//    snapshot.next_segment_at_timestamp(start).unwrap();
//    let seg = snapshot.get_segment();
//    let mut timestamps = seg.timestamps().iterator();
//    let ts = timestamps.next().unwrap();
//    if ts > start {
//        break;
//    }
//}

//let data_latency = timer.elapsed();

////println!("Executing query");
//ThreadLocalTimer::reset();
//ThreadLocalCounter::reset();
//let result_count = {
//    let _timer_1 = ThreadLocalTimer::new("query execution");
//    let snapshot_id = runtime.block_on(client.get(snapshotter_id)).unwrap();
//    let mut snapshot = snapshot_id.load().into_iterator();
//    //println!("Range: {} {}", start, end);
//    let mut count = 0;
//    'outer: loop {
//        if snapshot.next_segment_at_timestamp(start).is_none() {
//            break;
//        }
//        let seg = snapshot.get_segment();
//        let mut timestamps = seg.timestamps().iterator();
//        for (i, ts) in timestamps.enumerate() {
//            count += 1;
//            if ts < end {
//                break 'outer;
//            }
//        }
//    }
//    count
//};
//let total_latency = timer.elapsed();
//let kafka_fetch = {
//    let timers = ThreadLocalTimer::timers();
//    timers
//        .get("ReadOnlyBlock::as_bytes")
//        .unwrap()
//        .clone()
//        .as_secs_f64()
//};
//let mut timers: Vec<(String, Duration)> = ThreadLocalTimer::timers()
//    .iter()
//    .map(|(s, d)| (s.clone(), *d))
//    .collect();
//timers.sort();
////println!("TIMERS\n{:?}", timers);

//let query_latency = total_latency - data_latency;
//let total_query = start_delay + to_end;

////let uncached_blocks_read = snapshot.uncached_blocks_read();
//let counters = ThreadLocalCounter::counters();
//let blocks_skipped = counters.get("skipping block").or(Some(&0)).unwrap();
//let blocks_loaded = counters.get("loading block").or(Some(&0)).unwrap();
//let segments_skipped = counters.get("skipping segment").or(Some(&0)).unwrap();
//let segments_loaded = counters.get("loading segment").or(Some(&0)).unwrap();
//let fetch_requests = counters.get("kafka fetch").or(Some(&0)).unwrap();
////let cached_messages = counters.get("cached kafka messages read").or(Some(&0)).unwrap();
//let kafka_messages_read = counters.get("kafka messages fetched").or(Some(&0)).unwrap();

//println!(
//    "Query id: {}, Total Time: {:?}, Data Latency: {:?}, Query Latency: {:?}, Blocks Skipped: {}, Blocks Read: {}, Segments Read: {}, Start Delay: {}, Query Execution Range: {}, Total Query Range: {}, Kafka Fetch Time: {:?}, Fetch Requests: {:?}, Kafka messages read: {}, Result: {}",
//    query_id,
//    total_latency.as_secs_f64(),
//    data_latency.as_secs_f64(),
//    query_latency.as_secs_f64(),
//    blocks_skipped,
//    blocks_loaded,
//    segments_loaded,
//    start_delay,
//    to_end,
//    total_query,
//    kafka_fetch,
//    fetch_requests,
//    kafka_messages_read,
//    //cached_messages,
//    result_count,
//);

//let mut kafka_client = Client::new(BOOTSTRAPS);
//let consumer_offset = ConsumerOffset::Latest;
//let mut consumer = BufferedConsumer::new(BOOTSTRAPS, TOPIC, consumer_offset);

// Freshness query
//loop {
//    let start: usize = micros_from_epoch().try_into().unwrap();
//    let snapshot_id = runtime.block_on(client.get(snapshotter_id)).unwrap();
//    let mut snapshot = snapshot_id.load(&mut kafka_client).into_iterator(&mut consumer);
//    snapshot.next_segment().unwrap();
//    let seg = snapshot.get_segment();
//    let mut timestamps = seg.timestamps().iterator();
//    let ts: usize = timestamps.next_timestamp().unwrap().try_into().unwrap();
//    let end: usize = micros_from_epoch().try_into().unwrap();
//    let duration = Duration::from_micros((end - start) as u64);
//    let age = Duration::from_micros((start - ts) as u64);
//    println!("snapshot id: {:?}, query latency: {:?}, data age: {:?}", snapshot_id, duration, age);
//    std::thread::sleep(Duration::from_secs(1));
//}

// Query the number of samples from a source for the past 5 minutes

//let ts: usize = timestamps.next_timestamp().unwrap().try_into().unwrap();
//}
//let end: usize = micros_from_epoch().try_into().unwrap();
//let duration = Duration::from_micros((end - start) as u64);
//let age = Duration::from_micros((start - ts) as u64);
//println!("snapshot id: {:?}, query latency: {:?}, data age: {:?}", snapshot_id, duration, age);
