use crossbeam::channel::{unbounded, Receiver, Sender};
use mach::{id::SeriesId, sample::SampleType};
use otlp::ResourceMetrics;
use std::collections::{hash_map::DefaultHasher, HashMap, HashSet};
use std::convert::TryInto;
use std::env;
use std::fs::{self, File};
use std::hash::{Hash, Hasher};
use std::io::{self, BufRead, *};
use std::path::{Path, PathBuf};

#[derive(Hash, Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
struct Attributes(Vec<(String, String)>);

impl Attributes {
    fn from_otel_attributes(attribs: &serde_json::Value) -> Self {
        let mut vec = Vec::new();
        for attrib in attribs.as_array().unwrap().iter() {
            vec.push((
                attrib["key"].as_str().unwrap().into(),
                attrib["value"]["stringValue"].as_str().unwrap().into(),
            ));
        }
        vec.sort();
        Attributes(vec)
    }

    fn from_jaeger_tags(attribs: &serde_json::Value) -> Self {
        let mut vec = Vec::new();

        for attrib in attribs.as_array().unwrap().iter() {
            let key = attrib["key"].as_str().unwrap().into();
            let value = {
                if !attrib["vInt64"].is_null() {
                    attrib["vInt64"].as_str().unwrap().into()
                } else if !attrib["vStr"].is_null() {
                    attrib["vStr"].as_str().unwrap().into()
                } else if !attrib["vFloat64"].is_null() {
                    format!("{}", attrib["vFloat64"].as_f64().unwrap())
                } else if !attrib["vBool"].is_null() {
                    match attrib["vBool"].as_bool().unwrap() {
                        true => "true".into(),
                        false => "false".into(),
                    }
                } else {
                    print_keys(attrib);
                    panic!("Missed an attrib key");
                }
            };
            vec.push((key, value));
        }
        vec.sort();
        Attributes(vec)
    }

    fn insert(&mut self, key: String, value: String) {
        self.0.push((key, value));
        self.0.sort();
    }

    fn remove(&mut self, key: &str) {
        let mut idx = 0;
        let mut missing = true;
        for item in self.0.iter() {
            if item.0.as_str() == key {
                missing = false;
                break;
            }
            idx += 1;
        }
        if missing {
            panic!();
        }
        assert_eq!(self.0.remove(idx).0, key);
        self.0.sort();
    }

    fn id(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

#[derive(Debug, Clone)]
struct MetricSample {
    id: u64,
    timestamp: u64,
    value: HashMap<u64, f64>,
}

impl MetricSample {
    fn vec_to_samples(mut data: Vec<Self>) -> Vec<(SeriesId, u64, Vec<SampleType>)> {
        let mut fields = HashMap::new();

        // get all fields from data
        for item in data.iter() {
            let f = fields.entry(item.id).or_insert(Vec::new());
            for k in item.value.keys() {
                f.push(*k);
            }
        }

        fields.iter_mut().for_each(|x| {
            x.1.sort();
            x.1.dedup();
        });

        let mut result = Vec::new();

        // fill with NAN if field is missing
        for item in data.iter_mut() {
            let id = SeriesId(item.id);
            let ts = item.timestamp;
            let mut values = Vec::new();

            let fields = fields.get(&item.id).unwrap();
            for field in fields.iter() {
                let v = *item.value.entry(*field).or_insert(f64::NAN);
                values.push(SampleType::F64(v));
            }
            result.push((id, ts, values));
        }
        result
    }

    fn into_sample(self) -> (SeriesId, u64, Vec<SampleType>) {
        let mut values = Vec::new();
        for item in self.value {
            values.push((item.0, SampleType::F64(item.1)));
        }
        values.sort_by(|a, b| a.0.cmp(&b.0));
        (
            SeriesId(self.id),
            self.timestamp,
            values.drain(..).map(|x| x.1).collect(),
        )
    }

    fn parse_otel_resource_metrics(json_data: &serde_json::Value) -> Vec<Self> {
        let mut all_points = Vec::new();
        for resource_metric in json_data["resourceMetrics"].as_array().unwrap().iter() {
            let resource_attribs =
                Attributes::from_otel_attributes(&resource_metric["resource"]["attributes"]);

            for scope in resource_metric["scopeMetrics"].as_array().unwrap().iter() {
                for metric in scope["metrics"].as_array().unwrap().iter() {
                    let metric_name = String::from(metric["name"].as_str().unwrap());

                    let mut hasher = DefaultHasher::new();
                    resource_attribs.hash(&mut hasher);
                    metric_name.hash(&mut hasher);
                    let id = hasher.finish();
                    let mut value = HashMap::new();
                    let mut timestamp = u64::MAX;

                    // Gauge
                    if !metric["gauge"].is_null() {
                        for point in metric["gauge"]["dataPoints"].as_array().unwrap().iter() {
                            let mut point_attribs =
                                Attributes::from_otel_attributes(&point["attributes"]);
                            point_attribs.remove("experiment_id");
                            let v = point["asDouble"].as_f64().unwrap();
                            value.insert(point_attribs.id(), v);
                            let ts = point["timeUnixNano"].as_str().unwrap().parse().unwrap();
                            if timestamp != u64::MAX {
                                assert_eq!(timestamp, ts);
                            } else {
                                timestamp = ts;
                            }
                        }
                    }
                    // Sum
                    else if !metric["sum"].is_null() {
                        for point in metric["sum"]["dataPoints"].as_array().unwrap().iter() {
                            let mut point_attribs =
                                Attributes::from_otel_attributes(&point["attributes"]);
                            point_attribs.remove("experiment_id");
                            let v = point["asDouble"].as_f64().unwrap();
                            value.insert(point_attribs.id(), v);
                            let ts = point["timeUnixNano"].as_str().unwrap().parse().unwrap();
                            if timestamp != u64::MAX {
                                assert_eq!(timestamp, ts);
                            } else {
                                timestamp = ts;
                            }
                        }
                    }
                    // Summary
                    else if !metric["summary"].is_null() {
                        let array = metric["summary"]["dataPoints"].as_array().unwrap();
                        assert_eq!(array.len(), 1);
                        for point in array.iter() {
                            let mut point_attribs =
                                Attributes::from_otel_attributes(&point["attributes"]);
                            point_attribs.remove("experiment_id");
                            let ts = point["timeUnixNano"].as_str().unwrap().parse().unwrap();
                            if timestamp != u64::MAX {
                                assert_eq!(timestamp, ts);
                            } else {
                                timestamp = ts;
                            }
                            let mut quantiles: Vec<(f64, f64)> = Vec::new();
                            for quantile_value in point["quantileValues"].as_array().unwrap().iter()
                            {
                                if quantile_value["quantile"].is_null()
                                    || quantile_value["quantile"] == "0.0"
                                {
                                    let v = quantile_value["value"].as_f64().unwrap();
                                    let mut attribs = point_attribs.clone();
                                    attribs.insert("quantile".into(), "0.0".into());
                                    value.insert(attribs.id(), v);
                                    //quantiles.push(("0.0".parse().unwrap(), v));
                                } else {
                                    let v = quantile_value["value"].as_f64().unwrap();
                                    let q = quantile_value["quantile"].as_f64().unwrap();
                                    let mut attribs = point_attribs.clone();
                                    attribs.insert("quantile".into(), format!("{}", q));
                                    value.insert(attribs.id(), v);
                                }
                            }
                        }
                    } else {
                        let keys: Vec<&String> = metric.as_object().unwrap().keys().collect();
                        panic!("Missed a metric: {:?}", keys);
                    }

                    all_points.push(MetricSample {
                        id,
                        timestamp,
                        value,
                    });
                }
            }
        }
        all_points
    }
}

fn print_keys(json_data: &serde_json::Value) {
    let keys: Vec<&String> = json_data.as_object().unwrap().keys().collect();
    println!("Keys: {:?}", keys);
}

struct LogSample {
    id: u64,
    timestamp: u64,
    value: Vec<u8>,
}

impl LogSample {
    fn parse_otel_resource_logs(json_data: &serde_json::Value) -> Vec<Self> {
        let mut samples = Vec::new();
        for resource_logs in json_data["resourceLogs"].as_array().unwrap().iter() {
            for scope_logs in resource_logs["scopeLogs"].as_array().unwrap().iter() {
                for log_record in scope_logs["logRecords"].as_array().unwrap().iter() {
                    let timestamp = log_record["timeUnixNano"]
                        .as_str()
                        .unwrap()
                        .parse()
                        .unwrap();
                    let mut id = 0;
                    for attrib in log_record["attributes"].as_array().unwrap().iter() {
                        if attrib["key"].as_str().unwrap() == "log.file.path" {
                            let to_hash = attrib["value"]["stringValue"].as_str().unwrap();
                            let mut hasher = DefaultHasher::new();
                            to_hash.hash(&mut hasher);
                            id = hasher.finish();
                            break;
                        }
                    }
                    assert!(id > 0);
                    let value = bincode::serialize(log_record).unwrap();
                    let sample = LogSample {
                        id,
                        timestamp,
                        value,
                    };
                    samples.push(sample);
                }
            }
        }
        samples
    }

    fn into_sample(self) -> (SeriesId, u64, Vec<SampleType>) {
        (
            SeriesId(self.id),
            self.timestamp,
            vec![SampleType::Bytes(self.value)],
        )
    }
}

struct SpanSample {
    id: u64,
    timestamp: u64,
    value: Vec<u8>,
}

impl SpanSample {
    fn into_sample(self) -> (SeriesId, u64, Vec<SampleType>) {
        (
            SeriesId(self.id),
            self.timestamp,
            vec![SampleType::Bytes(self.value)],
        )
    }

    fn parse_jaeger_span(span: &serde_json::Value) -> Self {
        let mut hasher = DefaultHasher::new();
        span["operationName"].as_str().unwrap().hash(&mut hasher);

        {
            let mut missing_span_kind = true;
            for item in span["tags"].as_array().unwrap().iter() {
                if item["key"].as_str().unwrap() == "span.kind" {
                    item["vStr"].as_str().unwrap().hash(&mut hasher);
                    missing_span_kind = false;
                    break;
                }
            }
            if missing_span_kind {
                panic!("missing span kind");
            }
        }

        {
            let mut missing_experiment_id = true;
            for item in span["tags"].as_array().unwrap().iter() {
                if item["key"].as_str().unwrap() == "experiment_id" {
                    item["vStr"].as_str().unwrap().hash(&mut hasher);
                    missing_experiment_id = false;
                    break;
                }
            }
            if missing_experiment_id {
                panic!("missing span kind");
            }
        }

        {
            let mut missing_experiment_id = true;
            for item in span["tags"].as_array().unwrap().iter() {
                if item["key"].as_str().unwrap() == "experiment_id" {
                    item["vStr"].as_str().unwrap().hash(&mut hasher);
                    missing_experiment_id = false;
                    break;
                }
            }
            if missing_experiment_id {
                panic!("missing span kind");
            }
        }

        span["process"]["serviceName"]
            .as_str()
            .unwrap()
            .hash(&mut hasher);
        Attributes::from_jaeger_tags(&span["process"]["tags"]).hash(&mut hasher);
        let id = hasher.finish();
        //let id = Attributes::from_jaeger_tags(&span["tags"]);
        let timestamp: u64 = {
            let s: &str = span["startTime"].as_str().unwrap();
            let t = chrono::DateTime::parse_from_rfc3339(s).unwrap();
            t.timestamp_millis().try_into().unwrap()
        };
        let value = bincode::serialize(span).unwrap();
        SpanSample {
            id,
            timestamp,
            value,
        }
    }
}

fn load_logs() -> Vec<(SeriesId, u64, Vec<SampleType>)> {
    println!("Parsing Logs");
    let dir = "/home/fsolleza/data/intel-telemetry/log_topic/partition=0";
    let mut all_points = Vec::new();
    for entry in fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let lines = {
            let file = File::open(entry.path()).unwrap();
            io::BufReader::new(file).lines()
        };
        for line in lines {
            if let Ok(line) = line {
                let json_data: serde_json::Value = serde_json::from_str(&line).unwrap();
                all_points.append(&mut LogSample::parse_otel_resource_logs(&json_data));
            }
        }
    }
    println!("Parsed n points: {}", all_points.len());

    let mut map = std::collections::HashMap::new();
    for item in all_points.iter() {
        map.entry(item.id).or_insert(Vec::new()).push(item);
    }
    println!("Number of keys: {}", map.len());

    all_points.drain(..).map(|x| x.into_sample()).collect()
}

fn load_traces() -> Vec<(SeriesId, u64, Vec<SampleType>)> {
    println!("Parsing Traces");
    let dir = "/home/fsolleza/data/intel-telemetry/trace_topic/partition=0";
    let mut all_points = Vec::new();
    for entry in fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let lines = {
            let file = File::open(entry.path()).unwrap();
            io::BufReader::new(file).lines()
        };
        for line in lines {
            if let Ok(line) = line {
                let json_data: serde_json::Value = serde_json::from_str(&line).unwrap();
                all_points.push(SpanSample::parse_jaeger_span(&json_data));
            }
        }
    }
    println!("Parsed n points: {}", all_points.len());

    let mut map = std::collections::HashMap::new();
    for item in all_points.iter() {
        map.entry(item.id.clone()).or_insert(Vec::new()).push(item);
    }

    println!("Number of keys: {}", map.len());
    all_points.drain(..).map(|x| x.into_sample()).collect()
}

fn load_metrics() -> Vec<(SeriesId, u64, Vec<SampleType>)> {
    let dir = "/home/fsolleza/data/intel-telemetry/metric_topic/partition=0";
    let mut all_points = Vec::new();
    let (path_tx, path_rx): (Sender<PathBuf>, Receiver<PathBuf>) = unbounded();
    let (tx, rx): (Sender<Vec<MetricSample>>, Receiver<Vec<MetricSample>>) = unbounded();
    let mut to_join = Vec::new();
    for _ in 0..32 {
        let path_rx = path_rx.clone();
        let tx = tx.clone();
        to_join.push(std::thread::spawn(move || {
            while let Ok(path) = path_rx.recv() {
                let path2 = path.clone();
                let lines = {
                    let file = File::open(path).unwrap();
                    io::BufReader::new(file).lines()
                };
                for line in lines {
                    if let Ok(line) = line {
                        let json_data: serde_json::Value = serde_json::from_str(&line).unwrap();
                        let mut points = MetricSample::parse_otel_resource_metrics(&json_data);
                        tx.send(points).unwrap();
                    }
                }
            }
        }));
    }
    for entry in fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path().clone();
        path_tx.send(path).unwrap();
    }
    drop(path_rx);
    drop(path_tx);
    drop(tx);
    while let Ok(mut x) = rx.recv() {
        all_points.append(&mut x);
    }

    println!("Waiting for threads joining");
    for item in to_join {
        item.join();
    }

    println!("Getting some stats");
    let mut map = HashMap::new();
    for item in all_points.iter() {
        map.entry(item.id).or_insert(Vec::new()).push(item);
    }

    println!("Number of keys: {}", map.len());

    let mut lengths = Vec::new();
    for (k, v) in map.iter() {
        lengths.push(v.len());
    }
    let min: usize = *lengths.iter().min().unwrap();
    let max: usize = *lengths.iter().max().unwrap();
    let mean: usize = lengths.iter().sum::<usize>() / lengths.len();
    println!("average length {} {} {}", min, max, mean);

    let mut all_points = MetricSample::vec_to_samples(all_points);

    println!("checking average field count");
    let mut len_map: HashMap<SeriesId, usize> = HashMap::new();
    for item in all_points.iter() {
        let len = *len_map.entry(item.0).or_insert(item.2.len());
        assert_eq!(len, item.2.len());
    }

    let min: usize = len_map.iter().map(|x| *x.1).min().unwrap();
    let max: usize = len_map.iter().map(|x| *x.1).max().unwrap();
    let mean: usize = len_map.iter().map(|x| *x.1).sum::<usize>() / len_map.len();
    println!("average fields {} {} {}", min, max, mean);

    // just printing to see the distribution of lengths

    //let mut len_hist = HashMap::new();
    //for (k, l) in len_map {
    //    *len_hist.entry(l).or_insert(0usize) += 1usize;
    //}
    //let mut len_hist: Vec<(usize, usize)> = len_hist.drain().collect();
    //len_hist.sort();
    //for item in len_hist {
    //    println!("{:?}", item);
    //}

    // for now, keep only less than 10 fields

    let mut keep = HashSet::new();
    for (k, l) in len_map {
        if l < 10 {
            keep.insert(*k);
        }
    }

    all_points
        .drain(..)
        .filter(|x| keep.contains(&x.0))
        .collect()
}

fn main() {
    let mut all_samples = Vec::new();
    all_samples.append(&mut load_logs());
    all_samples.append(&mut load_traces());
    all_samples.append(&mut load_metrics());

    println!("Getting some stats from all samples");
    println!("Number of points {}", all_samples.len());

    let mut map = HashMap::new();
    for item in all_samples.iter() {
        map.entry(item.0).or_insert(Vec::new()).push(item);
    }

    println!("Number of keys: {}", map.len());

    let mut lengths = Vec::new();
    for (k, v) in map.iter() {
        lengths.push(v.len());
    }
    let min: usize = *lengths.iter().min().unwrap();
    let max: usize = *lengths.iter().max().unwrap();
    let mean: usize = lengths.iter().sum::<usize>() / lengths.len();
    println!("average length {} {} {}", min, max, mean);

    all_samples.sort_by_key(|x| x.1);
    let bytes = bincode::serialize(&all_samples).unwrap();
    let mut file = File::create("/home/fsolleza/data/intel-telemetry/processed-data.bin").unwrap();
    file.write(bytes.as_slice()).unwrap();
    // Metrics data
}
