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

pub fn load_conf() -> Config {
    println!("LOADING CONFIG");
    let conf_path = match env::var("CONFIG") {
        Ok(val) => PathBuf::from(val),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("default-config")
            .join("config.yaml"),
    };
    let conf_string = std::fs::read_to_string(conf_path).unwrap();
    serde_yaml::from_str(&conf_string).unwrap()
}
