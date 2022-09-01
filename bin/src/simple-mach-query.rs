#[allow(dead_code)]
mod bytes_server;
#[allow(dead_code)]
mod snapshotter;

use mach::id::SeriesId;
use mach::snapshotter::SnapshotterId;
use mach::utils::kafka::init_thread_local_consumer;
use regex::Regex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::collections::HashMap;
use rand::prelude::*;
use mach::timer::*;


lazy_static::lazy_static! {
    static ref SNAPSHOT_INTERVAL: Duration = Duration::from_secs_f64(0.5);
    static ref SNAPSHOT_TIMEOUT: Duration = Duration::from_secs_f64(60. * 60.);
}

const START_MAX_DELAY: u64 = 60;
const MIN_QUERY_DURATION: u64 = 10;
const MAX_QUERY_DURATION: u64 = 60;
const SOURCE_COUNT: u64 = 1000;
const QUERY_COUNT: u64 = 1;

fn main() {
    mach::utils::kafka::init_kafka_consumer();
    // Sleeping to make sure there's enough data
    println!("Sleeping");
    std::thread::sleep(Duration::from_secs(2 * START_MAX_DELAY));
    println!("Done Sleeping");

    // Setup snapshot client
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut client = runtime.block_on(snapshotter::SnapshotClient::new());
    let interval = *SNAPSHOT_INTERVAL;
    let timeout = *SNAPSHOT_TIMEOUT;

    let mut snapshotter_map = HashMap::new();
    let micros_in_sec: u64 = Duration::from_secs(1).as_micros().try_into().unwrap();

    init_thread_local_consumer();

    let mut rng = rand::thread_rng();
    for _ in 0..QUERY_COUNT {
        let id = SeriesId(rng.gen_range(0..SOURCE_COUNT));
        let snapshotter_id = *snapshotter_map.entry(id).or_insert(
            runtime.block_on(client.initialize(id, interval, timeout)).unwrap()
        );
        let now: u64 = micros_from_epoch().try_into().unwrap();
        let start_delay = rng.gen_range(0..START_MAX_DELAY);
        let to_end = rng.gen_range(MIN_QUERY_DURATION..MAX_QUERY_DURATION);
        let start = now - start_delay * micros_in_sec;
        let end = start - to_end * micros_in_sec;

        let mut count = 0;

        ThreadLocalTimer::reset();
        let timer = Instant::now();
        let mut query_execution_time = Duration::from_secs(0);
        //println!("Waiting for data to be available");
        loop {
            let snapshot_id = runtime.block_on(client.get(snapshotter_id)).unwrap();
            let mut snapshot = snapshot_id.load().into_iterator();
            snapshot.next_segment().unwrap();
            let seg = snapshot.get_segment();
            let mut timestamps = seg.timestamps().iterator();
            let ts = timestamps.next().unwrap();
            if ts > start {
                break;
            }
        }

        let data_latency = timer.elapsed();

        //println!("Executing query");
        let snapshot = {
            let _timer_1 = ThreadLocalTimer::new("query execution");
            let snapshot_id = runtime.block_on(client.get(snapshotter_id)).unwrap();
            let mut snapshot = snapshot_id.load().into_iterator();
            //println!("Range: {} {}", start, end);
            let mut result = Vec::new();
            'outer: loop {
                if snapshot.next_segment().is_none() {
                    break;
                }
                let seg = snapshot.get_segment();
                let mut timestamps = seg.timestamps().iterator();
                let mut count = 0;
                for (i, ts) in timestamps.enumerate() {
                    result.push(ts);
                    if ts < end {
                        break 'outer;
                    }
                }
            }
            snapshot
        };
        let total_latency = timer.elapsed();
        let kafka_fetch = {
            let timers = ThreadLocalTimer::timers();
            timers.get("KafkaEntry::fetch").unwrap().clone().as_secs_f64()
        };
        let mut timers: Vec<(String, Duration)> = ThreadLocalTimer::timers().iter().map(|(s, d)| {
            (s.clone(), *d)
        }).collect();
        timers.sort();
        println!("TIMERS\n{:?}", timers);

        let query_latency = total_latency - data_latency;
        let total_query = start_delay + to_end;

        //let uncached_blocks_read = snapshot.uncached_blocks_read();
        let blocks_read = snapshot.blocks_read();
        let segments_read = snapshot.segments_read();
        //let messages_read = snapshot.messages_read();
        //let cached_messages_read = snapshot.cached_messages_read();

        println!(
            "Total Time: {:?}, Data Latency: {:?} Query Latency: {:?}, Blocks Read: {}, Segments Read: {}, Start Delay: {}, To End: {}, Total Query: {}, Kafka Fetch Time: {:?}",
            total_latency.as_secs_f64(),
            data_latency.as_secs_f64(),
            query_latency.as_secs_f64(),
            blocks_read,
            segments_read,
            start_delay,
            to_end,
            total_query,
            kafka_fetch
        );
    }

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
}

fn micros_from_epoch() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
}
