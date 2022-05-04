use serde::*;
use std::collections::HashMap;

#[derive(Clone, Serialize, Deserialize)]
pub enum Type {
    F64(f64),
    Str(String),
}

impl From<&Type> for mach::sample::Type {
    fn from(t: &Type) -> Self {
        let item = match t {
            Type::F64(x) => mach::sample::Type::F64(*x),
            Type::Str(x) => mach::sample::Type::Bytes(mach::utils::bytes::Bytes::from_slice(x.as_bytes())),
        };
        item
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Sample {
    pub tags: HashMap<String, String>,
    pub timestamp: u64,
    pub values: Vec<Type>,
}
