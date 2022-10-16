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
use rand::prelude::*;
use regex::Regex;
use std::collections::HashMap;
use std::sync::Arc;
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

async fn execute_query(i: usize, query: SimpleQuery, done_notifier: Sender<()>) {
    println!("Executing query: {}", i);
    let source = query.source;
    let start = query.start;
    let end = query.end;
    let interval = *SNAPSHOT_INTERVAL;
    let timeout = *SNAPSHOT_TIMEOUT;

    let mut client =
        snapshotter::SnapshotClient::new(PARAMETERS.snapshot_server_port.as_str()).await;

    let now = chrono::prelude::Utc::now();
    let timer = Instant::now();
    let snapshotter_id = *SNAPSHOTTER_MAP
        .entry(source)
        .or_insert(client.initialize(source, interval, timeout).await.unwrap())
        .value();
    let init_latency = timer.elapsed();

    // Wait for timestamp to be available
    loop {
        let now = Instant::now();
        let snapshot_id = client.get(snapshotter_id).await.unwrap();
        let mut snapshot = snapshot_id.load().into_iterator();
        snapshot.next_segment().unwrap();
        let seg = snapshot.get_segment();
        let mut timestamps = seg.timestamps().iterator();
        let ts = timestamps.next().unwrap();
        if ts > start {
            break;
        }
        tokio::time::sleep(interval * 3).await;
    }

    let data_latency = timer.elapsed();

    let result_count = {
        // let snapshot_id = client.get(snapshotter_id).await.unwrap();
        // let mut snapshot = snapshot_id.load().into_iterator();
        // let mut count = 0;
        // 'outer: loop {
        //     if snapshot.next_segment_at_timestamp(start).is_none() {
        //         break;
        //     }
        //     let seg = snapshot.get_segment();
        //     let mut timestamps = seg.timestamps().iterator();
        //     for (i, ts) in timestamps.enumerate() {
        //         count += 1;
        //         if ts < end {
        //             break 'outer;
        //         }
        //     }
        // }
        // count
        0
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
    print!("Init, Latency: {}, ", init_latency.as_secs_f64());
    print!("Sink: {}, ", result_count);
    println!("");

    done_notifier.send(()).unwrap();
}

#[tokio::main]
async fn main() {
    let mut rng = ChaCha8Rng::seed_from_u64(PARAMETERS.query_rand_seed);
    let num_sources: usize = (PARAMETERS.source_count / 10).try_into().unwrap();
    let sources = data_generator::HOT_SOURCES.as_slice();

    let mut start_notif = NotificationReceiver::new(PARAMETERS.querier_port);
    println!("Waiting for workload to start...");
    start_notif.wait();
    println!("Workload started");

    // Sleeping to make sure there's enough data
    let initial_sleep_secs = PARAMETERS.query_max_delay + PARAMETERS.query_max_duration;
    println!("Sleep for {initial_sleep_secs} seconds to wait for data arrival");
    std::thread::sleep(Duration::from_secs(initial_sleep_secs));
    println!("Done Sleeping");

    let start = Instant::now();
    let run_dur = Duration::from_secs(60);

    let (notification_channel, wait_notification_channel) = unbounded();

    let mut i = 0;
    while start.elapsed() < run_dur {
        thread::sleep(Duration::from_millis(10));
        let now: u64 = utils::timestamp_now_micros().try_into().unwrap();
        let query = {
            let mut q = SimpleQuery::new_relative_to(now);
            let source_idx = rng.gen_range(0..sources.len());
            q.source = sources[source_idx];
            q
        };

        let tx = notification_channel.clone();
        tokio::spawn(async move {
            execute_query(i, query, tx).await;
        });
        i += 1;
    }

    drop(notification_channel);
    while let Ok(_) = wait_notification_channel.recv() {}
}
