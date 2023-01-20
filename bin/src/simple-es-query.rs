// Copyright (c) 2023 Franco Solleza, Intel Corporation, Brown University
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#[allow(dead_code)]
mod constants;
#[allow(dead_code)]
mod data_generator;
#[allow(dead_code)]
mod elastic;
#[allow(dead_code)]
mod query_utils;
#[allow(dead_code)]
mod utils;

use constants::PARAMETERS;
use elastic::{ESClientBuilder, ESIndexQuerier, IngestStats};
use lazy_static::lazy_static;
use mach::{self, id::SourceId};
use query_utils::SimpleQuery;
use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::{
    cmp::{max, min},
    sync::{atomic::AtomicUsize, atomic::Ordering::SeqCst, Arc},
    time::{Duration, Instant},
};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

use crate::utils::NotificationReceiver;

lazy_static! {
    static ref INGESTION_STATS: Arc<IngestStats> = Arc::new(IngestStats::default());
    static ref PENDING_QUERY_COUNT: AtomicUsize = AtomicUsize::new(0);
    static ref ES_BUILDER: ESClientBuilder = ESClientBuilder::default()
        .endpoint(PARAMETERS.es_endpoint.clone())
        .username_optional(PARAMETERS.es_username.clone())
        .password_optional(PARAMETERS.es_password.clone());
}

#[derive(Debug)]
struct LatencyResult<T> {
    data_latency: Duration,
    total_latency: Duration,
    result: T,
}

#[derive(Debug)]
struct SeriesTimerangeDocCount {
    series: SourceId,
    start: u64,
    end: u64,
    doc_count: u64,
}

async fn wait_for_series_timestamp(querier: &ESIndexQuerier, series_id: u64, timestamp: u64) {
    let mut wait_secs = 1;
    let max_wait_secs = 120;
    let min_wait_secs = 1;
    loop {
        match querier
            .query_series_latest_timestamp(PARAMETERS.es_index_name.as_str(), series_id)
            .await
        {
            Ok(resp) => match resp {
                Some(ts) => {
                    if ts >= timestamp {
                        break;
                    } else {
                        let lag_micros = timestamp - ts;
                        let lag_seconds: f64 = lag_micros as f64 / 1_000_000.0;

                        wait_secs = min(max_wait_secs, max(min_wait_secs, lag_seconds as u64 / 10));

                        println!(
                            "Series {series_id} lags {lag_seconds}, will sleep {wait_secs} secs"
                        );
                    }
                }
                None => {
                    println!("Series {series_id} has no data yet");
                }
            },
            Err(e) => {
                println!("query latest timestamp err: {:?}", e);
            }
        }

        tokio::time::sleep(Duration::from_secs(wait_secs)).await;
    }
}

async fn exec_query(
    query_id: u64,
    query: SimpleQuery,
    done_notifier: UnboundedSender<LatencyResult<SeriesTimerangeDocCount>>,
) {
    PENDING_QUERY_COUNT.fetch_add(1, SeqCst);

    let querier = ESIndexQuerier::new(ES_BUILDER.clone().build().unwrap());

    let series_id = query.source.0;
    println!(
        "Start query {}, series {}, from now: {}, timerange [{}, {}]",
        query_id, series_id, query.from_now, query.start, query.end
    );

    let timer = Instant::now();
    wait_for_series_timestamp(&querier, series_id, query.end).await;
    let data_latency = timer.elapsed();

    let r = querier
        .query_series_doc_count(
            PARAMETERS.es_index_name.as_str(),
            series_id,
            query.end,
            query.start,
        )
        .await;
    let total_latency = timer.elapsed();

    PENDING_QUERY_COUNT.fetch_sub(1, SeqCst);

    assert!(r.is_ok(), "Query failed");
    let r = r.unwrap();
    assert!(r.is_some(), "Failed to parse response");

    let r = LatencyResult {
        data_latency,
        total_latency,
        result: SeriesTimerangeDocCount {
            series: query.source,
            start: query.end,
            end: query.start,
            doc_count: r.unwrap(),
        },
    };
    let query_latency = r.total_latency - r.data_latency;
    let num_pending_queries = PENDING_QUERY_COUNT.load(SeqCst);
    println!(
        "Query {}, Data latency secs: {}, Query latency secs: {}, \
            Total latency secs {}; result: {}, series {} [{}, {}]; {} pending queries",
        query_id,
        r.data_latency.as_secs_f64(),
        query_latency.as_secs_f64(),
        r.total_latency.as_secs_f64(),
        r.result.doc_count,
        r.result.series.0,
        r.result.start,
        r.result.end,
        num_pending_queries
    );

    done_notifier.send(r).unwrap();
}

fn read_data_in_background() {
    std::thread::spawn(|| {
        println!("start reading data in the background");
        let _data = &data_generator::HOT_SOURCES[..];
    });
}

#[tokio::main]
async fn main() {
    read_data_in_background();

    let mut start_notif = NotificationReceiver::new(PARAMETERS.querier_port);
    println!("Waiting for workload to start...");
    start_notif.wait();
    println!("Workload started");

    let mut rng = ChaCha8Rng::seed_from_u64(PARAMETERS.query_rand_seed);
    let num_sources: usize = (PARAMETERS.source_count / 10).try_into().unwrap();
    let sources = &data_generator::HOT_SOURCES[0..num_sources];

    let initial_sleep_secs = 2 * PARAMETERS.query_max_delay;
    println!("Sleep for {initial_sleep_secs} seconds to wait for data arrival");
    tokio::time::sleep(Duration::from_secs(initial_sleep_secs)).await;
    println!("Done sleeping");

    let (done_tx, mut done_rx) = unbounded_channel();
    for i in 0..PARAMETERS.query_count {
        tokio::time::sleep(Duration::from_secs(PARAMETERS.query_interval_seconds)).await;
        let now = utils::timestamp_now_micros();
        let mut query = SimpleQuery::new_relative_to(now);

        let source_idx = rng.gen_range(0..sources.len());
        query.source = sources[source_idx];

        let done_tx = done_tx.clone();
        tokio::spawn(async move {
            exec_query(i, query, done_tx).await;
        });
    }

    drop(done_tx);
    while let Some(_) = done_rx.recv().await {}
}
