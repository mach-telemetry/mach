use std::collections::HashMap;
use serde::*;

#[derive(Serialize, Deserialize)]
pub enum Type {
    F64(f64),
    Str(String)
}

#[derive(Serialize, Deserialize)]
pub struct Sample {
    pub tags: HashMap<String, String>,
    pub timestamp: u64,
    pub values: Vec<Type>,
}


