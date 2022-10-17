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

use crossbeam_channel::bounded;
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

fn execute_query(id: usize, rx: Receiver<(usize, SimpleQuery)>) {
    println!("Starting thread {id}");

    while let Ok((i, query)) = rx.recv() {
        println!("Executing query {i}");
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

        let now = chrono::prelude::Utc::now();
        let timer = Instant::now();
        let snapshotter_id = *SNAPSHOTTER_MAP
            .entry(source)
            .or_insert(
                runtime
                    .block_on(client.initialize(source, interval, timeout))
                    .unwrap(),
            )
            .value();
        let init_latency = timer.elapsed();

        let snapshot_id = runtime.block_on(client.get(snapshotter_id)).unwrap();

        let data_latency = timer.elapsed();

        let result_count = { 0 };
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
    }

    println!("thread ended");
}

fn main() {
    let mut rng = ChaCha8Rng::seed_from_u64(PARAMETERS.query_rand_seed);
    let num_sources: usize = (PARAMETERS.source_count / 10).try_into().unwrap();
    let sources = data_generator::HOT_SOURCES.as_slice();

    let mut start_notif = NotificationReceiver::new(PARAMETERS.querier_port);
    println!("Waiting for workload to start...");
    start_notif.wait();
    println!("Workload started");

    let (tx, rx) = unbounded();

    let mut handles = Vec::new();
    for id in 0..64 {
        let rx = rx.clone();
        handles.push(thread::spawn(move || {
            execute_query(id, rx);
        }));
    }

    let start = Instant::now();
    let run_dur = Duration::from_secs(60);
    let mut i = 0;
    let query_per_sec = 10000;
    while start.elapsed() < run_dur {
        for _ in 0..query_per_sec {
            let now: u64 = utils::timestamp_now_micros().try_into().unwrap();
            let query = {
                let mut q = SimpleQuery::new_relative_to(now);
                let source_idx = rng.gen_range(0..sources.len());
                q.source = sources[source_idx];
                q
            };
            tx.send((i, query));
            // match tx.try_send((i, query)) {
            //     Ok(_) => {}
            //     Err(e) => {
            //         eprintln!("send error: {:?}", e);
            //     }
            // }
            i += 1;
        }
        thread::sleep(Duration::from_secs(1));
    }

    drop(tx);
    println!("FINISHED ISSUING REQUESTS");
    for h in handles {
        h.join().unwrap();
    }
}
