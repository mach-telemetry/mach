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
use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::thread;
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
    let mut client = runtime.block_on(snapshotter::SnapshotClient::new(
        PARAMETERS.snapshot_server_port.as_str(),
    ));

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
