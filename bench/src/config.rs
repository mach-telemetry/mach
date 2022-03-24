use serde::*;
use std::env;
use std::path::PathBuf;

#[derive(Debug, Serialize, Deserialize)]
pub enum Variate {
    Univariate,
    Multivariate,
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
    pub item_definition_path: PathBuf,
}

impl Default for Config {
    fn default() -> Self {
        let cargo_manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let data_path = cargo_manifest.join("data");
        let out_path = cargo_manifest.join("out");
        let item_definition_path = cargo_manifest.join("default-config").join("item.rs");
        Config {
            zipf: 0.99,
            univariate: Variate::Univariate,
            threads: 1,
            segments: 1,
            series_per_thread: 100_000,
            samples_per_thread: 10_000_000,
            data_path,
            out_path,
            item_definition_path,
        }
    }
}

pub fn load_conf() -> Config {
    let conf_path = match env::var("CONFIG") {
        Ok(val) => PathBuf::from(val),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("default-config")
            .join("config.yaml"),
    };
    let conf_string = std::fs::read_to_string(conf_path).unwrap();
    let c = serde_yaml::from_str(&conf_string).unwrap();
    c
}
