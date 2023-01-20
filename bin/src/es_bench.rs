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
mod utils;

use crate::elastic::ESSample;
use constants::PARAMETERS;
use elastic::{ESBatchedIndexClient, ESClientBuilder, ESFieldType, ESIndexQuerier, IngestStats};
use lazy_static::lazy_static;
use std::sync::atomic::Ordering::SeqCst;
use std::{
    collections::HashMap,
    sync::{atomic::AtomicUsize, Arc},
    time::{Duration, Instant},
};
use tokio::sync::Barrier;
use utils::timestamp_now_micros;

lazy_static! {
    static ref SAMPLES: Vec<ESSample> = data_generator::SAMPLES
        .iter()
        .map(|sample| { ESSample::new(sample.0, timestamp_now_micros(), sample.1.to_vec()) })
        .collect();
    static ref INGESTION_STATS: Arc<IngestStats> = Arc::new(IngestStats::default());
    static ref INDEXED_COUNT: AtomicUsize = AtomicUsize::new(0);
    static ref BENCH_DUR_SECS: u64 = 600;
}

async fn bench(
    barr: Arc<Barrier>,
    es_builder: ESClientBuilder,
    batch_bytes: usize,
    run_duration: Duration,
) {
    let mut client = ESBatchedIndexClient::<ESSample>::new(
        es_builder.build().unwrap(),
        PARAMETERS.es_index_name.to_string(),
        batch_bytes,
        INGESTION_STATS.clone(),
    );

    let print_responses = false;

    barr.wait().await;
    let start = Instant::now();
    'outer: loop {
        for item in SAMPLES.iter() {
            match client.ingest(item.clone()).await {
                elastic::IngestResponse::Batched => (),
                elastic::IngestResponse::Flushed(r) => match r {
                    Err(e) => println!("Flush error: {:?}", e),
                    Ok(r) => {
                        if !r.status_code().is_success() {
                            println!("Flush error {}", r.status_code().as_u16());
                        }
                        if print_responses {
                            let resp = r.text().await.unwrap();
                            println!("{resp}");
                        }
                    }
                },
            }
            if start.elapsed() > run_duration {
                break 'outer;
            }
        }
    }
    client.flush().await.unwrap();
    drop(client);
    barr.wait().await;
}

#[allow(dead_code)]
async fn _series_querier(barr: Arc<Barrier>, es_builder: ESClientBuilder, run_duration: Duration) {
    let one_sec = Duration::from_secs(1);
    let client = ESIndexQuerier::new(es_builder.build().unwrap());
    barr.wait().await;
    let start = Instant::now();
    let mut idx = 0;
    while start.elapsed() < run_duration {
        let picked_series = SAMPLES[idx].series_id.0;
        idx += 1;
        match client
            .query_series_latest_timestamp(PARAMETERS.es_index_name.as_str(), picked_series)
            .await
        {
            Err(e) => println!("{:?}", e),
            Ok(doc_count) => {
                if let Some(doc_count) = doc_count {
                    println!("Resp for series {picked_series}: {doc_count}");
                }
            }
        }
        tokio::time::sleep(one_sec).await;
    }
    barr.wait().await;
}

async fn progress_watcher(barr: Arc<Barrier>, es_builder: ESClientBuilder, run_duration: Duration) {
    let one_sec = Duration::from_secs(1);
    let client = ESIndexQuerier::new(es_builder.build().unwrap());
    barr.wait().await;
    let start = Instant::now();
    while start.elapsed() < run_duration {
        match client
            .query_index_doc_count(PARAMETERS.es_index_name.as_str())
            .await
        {
            Err(e) => println!("{:?}", e),
            Ok(res) => {
                if res.is_some() {
                    INDEXED_COUNT.store(res.unwrap().try_into().unwrap(), SeqCst);
                }
            }
        }
        tokio::time::sleep(one_sec).await;
    }
    barr.wait().await;
}

#[tokio::main]
async fn main() {
    let _ = SAMPLES.len();

    let elastic_builder = ESClientBuilder::default()
        .endpoint(PARAMETERS.es_endpoint.clone())
        .username_optional(PARAMETERS.es_username.clone())
        .password_optional(PARAMETERS.es_password.clone());

    let client = ESBatchedIndexClient::<ESSample>::new(
        elastic_builder.clone().build().unwrap(),
        PARAMETERS.es_index_name.clone(),
        PARAMETERS.es_batch_bytes,
        INGESTION_STATS.clone(),
    );

    let mut schema = HashMap::new();
    schema.insert("series_id".into(), ESFieldType::Keyword);

    let r = client
        .create_index(elastic::CreateIndexArgs {
            num_shards: PARAMETERS.es_num_shards,
            num_replicas: PARAMETERS.es_num_replicas,
            schema: Some(schema),
        })
        .await
        .unwrap();
    println!("Create index response: {:?}", r);
    assert!(r.status_code().is_success());
    println!("index created; name: {}", PARAMETERS.es_index_name.as_str());

    let num_writers: usize = PARAMETERS.kafka_partitions.try_into().unwrap();

    let run_duration = Duration::from_secs(*BENCH_DUR_SECS);
    let barr = Arc::new(Barrier::new(num_writers + 2));
    for _ in 0..num_writers {
        let barr = barr.clone();
        let es_builder = elastic_builder.clone();
        let batch_size = PARAMETERS.es_batch_bytes;
        tokio::spawn(async move {
            bench(barr, es_builder, batch_size, run_duration).await;
        });
    }

    let barr_clone = barr.clone();
    let es_builder = elastic_builder.clone();
    tokio::spawn(async move {
        _series_querier(barr_clone, es_builder, run_duration).await;
    });

    barr.wait().await;
    let one_sec = Duration::from_secs(1);
    let start = Instant::now();
    while start.elapsed() < run_duration {
        let num_flushed = INGESTION_STATS.num_flushed.load(SeqCst);
        let num_indexed = INDEXED_COUNT.load(SeqCst);
        let fraction_indexed: f64 = num_indexed as f64 / num_flushed as f64;
        println!("num flushed: {num_flushed}, num indexed: {num_indexed}, fraction indexed: {fraction_indexed}");
        tokio::time::sleep(one_sec).await;
    }
    barr.wait().await;

    let bench_dur = start.elapsed();

    let num_flushed = INGESTION_STATS.num_flushed.load(SeqCst);
    let bytes_flushed = INGESTION_STATS.bytes_flushed.load(SeqCst);
    let num_flush_completed = INGESTION_STATS.num_flush_reqs_completed.load(SeqCst);
    let flush_ms = INGESTION_STATS.flush_dur_millis.load(SeqCst);
    let samples_per_sec = num_flushed as f64 / bench_dur.as_secs_f64();
    let bytes_per_sec = bytes_flushed as f64 / bench_dur.as_secs_f64();
    let avg_batch_size = num_flushed as f64 / num_flush_completed as f64;
    let avg_flush_ms = flush_ms as f64 / num_flush_completed as f64;
    let avg_sample_flush_ms = flush_ms as f64 / num_flushed as f64;

    println!(
        "bench duration: {}, number of samples: {}, samples per sec: {samples_per_sec}, \
        bytes per sec {bytes_per_sec}, average batch size: {avg_batch_size}, \
        average flush ms {avg_flush_ms}, \
        average sample flush ms {avg_sample_flush_ms}",
        bench_dur.as_secs_f64(),
        num_flushed
    );
}
