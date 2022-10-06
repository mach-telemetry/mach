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

use clap::*;
use constants::PARAMETERS;
use elastic::{ESClientBuilder, ESIndexQuerier, IngestStats};
use lazy_static::lazy_static;
use mach::{self, id::SeriesId};
use query_utils::SimpleQuery;
use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::{
    sync::{atomic::AtomicUsize, atomic::Ordering::SeqCst, Arc},
    time::{Duration, Instant},
};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

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
    series: SeriesId,
    start: u64,
    end: u64,
    doc_count: u64,
}

async fn wait_for_series_timestamp(querier: &ESIndexQuerier, series_id: u64, timestamp: u64) {
    let one_sec = std::time::Duration::from_secs(1);
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
                        println!("Series {series_id} lags {lag_seconds}");
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
        tokio::time::sleep(one_sec).await;
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
        "Querying series {}, timerange [{}, {}]",
        series_id, query.start, query.end
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
            Total latency secs {}; series {} has {} docs in timerange [{}, {}]; {} pending queries",
        query_id,
        r.data_latency.as_secs_f64(),
        query_latency.as_secs_f64(),
        r.total_latency.as_secs_f64(),
        r.result.series.0,
        r.result.doc_count,
        r.result.start,
        r.result.end,
        num_pending_queries
    );

    done_notifier.send(r).unwrap();
}

#[tokio::main]
async fn main() {
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
