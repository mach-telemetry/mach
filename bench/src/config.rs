use serde::*;
use std::path::PathBuf;

#[derive(Debug, Serialize, Deserialize)]
pub enum Variate {
    Univariate,
    Multivariate
}

pub enum Backend {
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub zipf: f64,
    pub univariate: Variate,
    pub threads: usize,
    pub segments: usize,
    pub series_per_thread: usize,
    pub samples_per_thread: usize,
    pub data_path: PathBuf,
    pub out_path: PathBuf,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            zipf: 0.99,
            univariate: Variate::Univariate,
            threads: 1,
            segments: 1,
            series_per_thread: 100_000,
            samples_per_thread: 10_000_000,
            data_path: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data"),
            out_path: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("out"),
        }
    }
}
