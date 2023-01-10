use serde::*;
use fxhash::FxHasher64;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use ordered_float::OrderedFloat;
use mach::sample::SampleType;
use mach::source::SourceId;

#[derive(Serialize, Deserialize, Copy, Clone, Debug)]
pub enum OtelType {
    Metric,
    Log,
    Span
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Point {
    pub otel_type: OtelType,
    pub attributes: BTreeMap<String, Value>,
    pub timestamp: u64,
    pub values: Vec<Value>
}

impl Point {
    pub fn source_id(&self) -> SourceId {
        let mut map: BTreeMap<&str, HashableValue> = self.attributes.iter().map(|x| {
            (x.0.as_str(), x.1.into())
        }).collect();
        let mut hasher = FxHasher64::default();
        map.hash(&mut hasher);
        SourceId(hasher.finish())
    }

    pub fn mach_sample(&self) -> (SourceId, u64, Vec<SampleType>) {
        let id = self.source_id();
        let values = self.values.iter().map(|x| x.into()).collect();
        (id, self.timestamp, values)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Value {
    String(String),
    Int(i64),
    Uint(u64),
    Float(f64),
    Bool(bool),
    Null,
}

impl Into<SampleType> for &Value {
    fn into(self) -> SampleType {
        match self {
            Value::String(x) => SampleType::Bytes(x.as_bytes().into()),
            Value::Int(x) => SampleType::I64(*x),
            Value::Uint(x) => SampleType::U64(*x),
            Value::Float(x) => SampleType::F64(*x),
            _ => panic!("unhandled type"),
        }
    }
}

#[derive(Eq, Hash, PartialEq)]
enum HashableValue {
    String(String),
    Int(i64),
    Uint(u64),
    Float(OrderedFloat<f64>),
    Bool(bool),
    Null,
}

impl From<&Value> for HashableValue {
    fn from(item: &Value) -> Self {
        match item {
            Value::String(x) => Self::String(x.clone()),
            Value::Int(x) => Self::Int(*x),
            Value::Uint(x) => Self::Uint(*x),
            Value::Float(x) => Self::Float(OrderedFloat(*x)),
            Value::Bool(x) => Self::Bool(*x),
            Value::Null => Self::Null,
        }
    }
}
