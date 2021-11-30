//use crate::tsdb::{Fl, Sample};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Clone)]
pub struct Sample {
    pub ts: u64,
    pub values: Box<[f64]>,
}

pub const UNIVARIATE: &str = "bench1_univariate_small.json";
pub const MULTIVARIATE: &str = "bench1_multivariate_small.json";
pub const MIN_SAMPLES: usize = 30_000;

lazy_static! {
    pub static ref TEST_DATA_PATH: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data");
    pub static ref UNIVARIATE_DATA: Arc<Vec<(String, Data)>> = Arc::new(load_univariate());
    pub static ref MULTIVARIATE_DATA: Arc<Vec<(String, Data)>> = Arc::new(load_multivariate());
}

pub type Data = Arc<Vec<Sample>>;

#[derive(Serialize, Deserialize)]
pub struct DataEntry {
    timestamps: Vec<u64>,
    values: HashMap<String, Vec<f64>>,
}

pub fn load_univariate() -> Vec<(String, Data)> {
    println!("Loading univariate data");
    let mut file = OpenOptions::new()
        .read(true)
        .open(Path::new(TEST_DATA_PATH.as_path()).join(UNIVARIATE))
        .unwrap();
    let mut json = String::new();
    file.read_to_string(&mut json).unwrap();
    let dict: HashMap<String, DataEntry> = serde_json::from_str(json.as_str()).unwrap();

    println!("Making univariate data");
    let mut data = Vec::new();
    for (k, d) in dict.iter() {
        let item = make_univariate_samples(d);
        if item.len() > MIN_SAMPLES {
            data.push((k.clone(), item));
        }
    }
    data.sort_by_key(|x| x.0.clone());
    data
}

fn make_univariate_samples(data: &DataEntry) -> Data {
    for i in 1..data.timestamps.len() {
        assert!(data.timestamps[i - 1] < data.timestamps[i])
    }
    let values = data.values.get("values").unwrap();
    // check all timestamps for sortedness
    let mut samples = Vec::new();
    for (ts, val) in data.timestamps.iter().zip(values.iter()) {
        let v = vec![*val as f64].into_boxed_slice();
        let s = Sample { ts: *ts, values: v };
        samples.push(s);
    }
    Arc::new(samples)
}

pub fn load_multivariate() -> Vec<(String, Data)> {
    println!("Loading multivariate data");
    let mut file = OpenOptions::new()
        .read(true)
        .open(Path::new(TEST_DATA_PATH.as_path()).join(MULTIVARIATE))
        .unwrap();
    let mut json = String::new();
    file.read_to_string(&mut json).unwrap();
    let dict: HashMap<String, DataEntry> = serde_json::from_str(json.as_str()).unwrap();

    println!("Making multivariate data");
    let mut data = Vec::new();
    for (k, d) in dict.iter() {
        let item = make_multivariate_samples(d);
        if item.len() > MIN_SAMPLES {
            data.push((k.clone(), item));
        }
    }
    data.sort_by_key(|x| x.0.clone());
    data
}

fn make_multivariate_samples(data: &DataEntry) -> Data {
    for i in 1..data.timestamps.len() {
        assert!(data.timestamps[i - 1] < data.timestamps[i])
    }
    let mut cols: Vec<String> = data.values.keys().cloned().collect();
    cols.sort();
    let mut samples = Vec::new();
    for (idx, t) in data.timestamps.iter().enumerate() {
        let mut v = Vec::new();
        for c in cols.iter() {
            v.push(data.values.get(c).unwrap()[idx]);
        }
        samples.push(Sample {
            ts: *t,
            values: v.iter().map(|x| *x as f64).collect(),
        });
    }
    Arc::new(samples)
}
