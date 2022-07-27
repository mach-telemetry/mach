mod prep_data;

use std::{
    fmt::format,
    time::{SystemTime, UNIX_EPOCH},
};

use clap::*;
use elasticsearch::{http::request::JsonBody, BulkParts, Elasticsearch, IndexParts};
use lazy_static::lazy_static;
use mach::{id::SeriesId, sample::SampleType};
use serde::{Deserialize, Serialize};
use serde_json::json;

const BATCH_SIZE: usize = 1024;

lazy_static! {
    static ref ARGS: Args = Args::parse();
    static ref SAMPLES: Vec<ESSample> = prep_data::load_samples(ARGS.file_path.as_str())
        .into_iter()
        .map(|s| s.into())
        .collect();
}

#[derive(Serialize, Deserialize, Clone)]
struct ESSample {
    series_id: SeriesId,
    timestamp: u64,
    data: Vec<SampleType>,
}

impl From<prep_data::Sample> for ESSample {
    fn from(data: prep_data::Sample) -> ESSample {
        ESSample {
            series_id: data.0,
            timestamp: data.1,
            data: data.2,
        }
    }
}

impl Into<JsonBody<serde_json::Value>> for ESSample {
    fn into(self) -> JsonBody<serde_json::Value> {
        serde_json::to_value(self).unwrap().into()
    }
}

#[derive(Parser, Debug)]
struct Args {
    #[clap(short, long, default_value_t = String::from("/home/sli/data/train-ticket-data"))]
    file_path: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let index_name: String = format!(
        "test-data-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    let samples: Vec<ESSample> = prep_data::load_samples(ARGS.file_path.as_str())
        .into_iter()
        .map(|s| s.into())
        .collect();

    let client = Elasticsearch::default();

    println!("start writing to ES index {}", index_name);

    let mut batch_item_no = 0;
    let mut num_batches_written = 0;
    let mut body: Vec<JsonBody<_>> = Vec::with_capacity(BATCH_SIZE * 2);
    for sample in samples {
        body.push(json!({"index": {"_id": batch_item_no}}).into());
        body.push(sample.into());
        batch_item_no += 1;
        if batch_item_no == BATCH_SIZE {
            let r = client
                .bulk(BulkParts::Index(index_name.as_str()))
                .body(body)
                .send()
                .await?;
            assert!(r.error_for_status_code().is_ok());
            batch_item_no = 0;
            num_batches_written += 1;
            println!("Num batches written: {}", num_batches_written);
            body = Vec::with_capacity(BATCH_SIZE * 2);
        }
    }

    Ok(())
}
