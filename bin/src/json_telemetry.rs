use serde::*;
use fxhash::FxHasher64;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use ordered_float::OrderedFloat;
use mach::sample::SampleType;
use mach::source::SourceId;
use lazy_static::*;
use crate::constants::*;
use log::{debug, error, info};
use std::time::Instant;

lazy_static! {
    pub static ref DATA: Arc<Vec<TelemetrySample>> = Arc::new(load_data(PARAMETERS.file_path.as_str()));
}

pub fn load_data(path: &str) -> Vec<TelemetrySample> {
    use std::io::{*, BufReader, BufRead};
    use std::path::PathBuf;
    use std::fs::File;
    let mut samples: Vec<TelemetrySample> = Vec::new();
    let file_path = PathBuf::from(path);
    info!("Loading data from filepath: {}", path);
    let start = Instant::now();
    let f = File::open(file_path).unwrap();
    let mut reader = BufReader::new(f);
    for (idx, line) in reader.lines().enumerate() {
        let point: Point = serde_json::from_str(&line.unwrap()).unwrap();
        if !point.has_null() { // && point.otel_type == OtelType::Log {
            let sample: TelemetrySample = point.mach_sample();
            samples.push(sample);
        }
    }
    info!("Loading data took {:?}", Instant::now() - start);
    samples
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug, Eq, PartialEq)]
pub enum OtelType {
    Metric,
    Log,
    Span
}

pub struct TelemetrySample {
    pub otel_type: OtelType,
    pub source: SourceId,
    pub timestamp: u64,
    pub values: Vec<SampleType>,
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

    pub fn mach_sample(&self) -> TelemetrySample {
        let source = self.source_id();
        let values = self.values.iter().map(|x| x.into()).collect();
        TelemetrySample {
            otel_type: self.otel_type,
            source,
            timestamp: self.timestamp,
            values,
        }
    }

    pub fn has_null(&self) -> bool {
        for x in self.values.iter() {
            if x.is_null() {
                return true
            }
        }
        false
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

impl Value {
    fn is_null(&self) -> bool {
        match self {
            Value::Null => true,
            _ => false,
        }
    }
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

#[cfg(test)]
mod test {
    use super::*;
    use std::path::PathBuf;
    use std::fs::File;
    use std::io::{self, prelude::*, BufRead, BufReader};
    use std::path::Path;
    use serde_json::*;

    #[test]
    fn test_json_parser() {
        let file_path = PathBuf::from("/home/fsolleza/data/telemetry-samples-small");
        let f = File::open(file_path).unwrap();
        let mut reader = BufReader::new(f);
        let mut points: Vec<Point> = Vec::new();
        for (idx, line) in reader.lines().enumerate() {
            points.push(serde_json::from_str(&line.unwrap()).unwrap());
        }
        let mut samples: Vec<TelemetrySample> = Vec::new();
        let mut null_samples = 0;
        let mut metrics = 0;
        let mut logs = 0;
        let mut spans = 0;
        for point in points {
            if !point.has_null() {
                let sample: TelemetrySample = point.mach_sample();
                match sample.otel_type {
                    OtelType::Metric => metrics += 1,
                    OtelType::Log => logs += 1,
                    OtelType::Span => spans += 1,
                }
                samples.push(sample);
            } else {
                null_samples += 1;
            }
        }
        println!("Count: {}", samples.len());
        println!("Nulls: {}", null_samples);
        println!("Metrics: {}", metrics);
        println!("Logs: {}", logs);
        println!("Spans: {}", spans);
    }
}
