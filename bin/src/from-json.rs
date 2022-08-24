use otlp::ResourceMetrics;
use std::env;
use std::fs::{self, File};
use std::io::{self, BufRead};
use std::path::Path;
use std::convert::TryInto;

#[derive(Hash, Debug, Clone, PartialEq, Eq)]
struct Attributes(Vec<(String, String)>);

impl Attributes {
    fn from_otel_attributes(attribs: &serde_json::Value) -> Self {
        let mut vec = Vec::new();
        for attrib in attribs.as_array().unwrap().iter() {
            vec.push((attrib["key"].as_str().unwrap().into(), attrib["value"]["stringValue"].as_str().unwrap().into()));
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
                }
                else if !attrib["vStr"].is_null() {
                    attrib["vStr"].as_str().unwrap().into()
                }
                else if !attrib["vBool"].is_null() {
                    match attrib["vBool"].as_bool().unwrap() {
                        true => "true".into(),
                        false => "false".into(),
                    }
                }
                else {
                    print_keys(attrib);
                    panic!("Missed an attrib key");
                }
            };
            vec.push((key, value));
        }
        vec.sort();
        Attributes(vec)
    }
}

#[derive(Hash, Debug, Clone, PartialEq, Eq)]
struct MetricId {
    resource_attribs: Attributes,
    point_attribs: Attributes,
    metric_name: String,
}

#[derive(Debug, Clone)]
struct MetricSample {
    id: MetricId,
    timestamp: u64,
    value: Vec<f64>,
}

impl MetricSample {
    fn parse_otel_resource_metrics(json_data: &serde_json::Value) -> Vec<Self> {
        let mut all_points = Vec::new();
        for resource_metric in json_data["resourceMetrics"].as_array().unwrap().iter() {
            let resource_attribs = Attributes::from_otel_attributes(&resource_metric["resource"]["attributes"]);

            for scope in resource_metric["scopeMetrics"].as_array().unwrap().iter() {
                for metric in scope["metrics"].as_array().unwrap().iter() {
                    let name = String::from(metric["name"].as_str().unwrap());

                    // Gauge
                    if !metric["gauge"].is_null() {
                        for point in metric["gauge"]["dataPoints"].as_array().unwrap().iter() {
                            let point_attribs = Attributes::from_otel_attributes(&point["attributes"]);
                            let metric_id = MetricId {
                                resource_attribs: resource_attribs.clone(),
                                point_attribs,
                                metric_name: name.clone(),
                            };
                            let p = MetricSample {
                                id: metric_id,
                                timestamp: point["timeUnixNano"].as_str().unwrap().parse().unwrap(),
                                value: vec![point["asDouble"].as_f64().unwrap()],
                            };
                            all_points.push(p);
                        }
                    }

                    // Sum
                    else if !metric["sum"].is_null() {
                        for point in metric["sum"]["dataPoints"].as_array().unwrap().iter() {
                            let point_attribs = Attributes::from_otel_attributes(&point["attributes"]);
                            let metric_id = MetricId {
                                resource_attribs: resource_attribs.clone(),
                                point_attribs,
                                metric_name: name.clone(),
                            };
                            let p = MetricSample {
                                id: metric_id,
                                timestamp: point["timeUnixNano"].as_str().unwrap().parse().unwrap(),
                                value: vec![point["asDouble"].as_f64().unwrap()],
                            };
                            all_points.push(p);
                        }
                    }

                    // Summary
                    else if !metric["summary"].is_null() {
                        for point in metric["summary"]["dataPoints"].as_array().unwrap().iter() {
                            let point_attribs = Attributes::from_otel_attributes(&point["attributes"]);
                            let metric_id = MetricId {
                                resource_attribs: resource_attribs.clone(),
                                point_attribs,
                                metric_name: name.clone(),
                            };
                            let mut qv = Vec::new();
                            for quantile_value in point["quantileValues"].as_array().unwrap().iter() {
                                if quantile_value["quantile"].is_null() {
                                    qv.push((0.0, quantile_value["value"].as_f64().unwrap()));
                                } else {
                                    let q = quantile_value["quantile"].as_f64().unwrap();
                                    let v = quantile_value["value"].as_f64().unwrap();
                                    qv.push((q, v));
                                }
                            }
                            qv.sort_by(|a, b| a.partial_cmp(b).unwrap());
                            let value = qv.iter().map(|x| x.1).collect();

                            let p = MetricSample {
                                id: metric_id,
                                timestamp: point["timeUnixNano"].as_str().unwrap().parse().unwrap(),
                                value,
                            };
                            all_points.push(p);
                        }
                    }

                    else {
                        let keys: Vec<&String> = metric.as_object().unwrap().keys().collect();
                        panic!("Missed a metric: {:?}", keys);
                    }
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

struct SpanSample {
    id: Attributes,
    timestamp: u64,
    value: Box<[u8]>,
}

impl SpanSample {
    fn parse_jaeger_span(span: &serde_json::Value) -> Self {
        let id = Attributes::from_jaeger_tags(&span["tags"]);
        let timestamp: u64 = {
            let s: &str = span["startTime"].as_str().unwrap();
            let t = chrono::DateTime::parse_from_rfc3339(s).unwrap();
            t.timestamp_millis().try_into().unwrap()
        };
        let value = bincode::serialize(span).unwrap().into_boxed_slice();
        SpanSample {
            id,
            timestamp,
            value
        }
    }
}


fn main() {

    //let dir = "/home/fsolleza/data/intel/spans";
    //let mut all_points = Vec::new();
    //for entry in fs::read_dir(dir).unwrap() {
    //    let entry = entry.unwrap();
    //    let lines = {
    //        let file = File::open(entry.path()).unwrap();
    //        io::BufReader::new(file).lines()
    //    };
    //    println!("entry path: {:?}", entry.path());
    //    for line in lines {
    //        if let Ok(line) = line {
    //            let json_data: serde_json::Value = serde_json::from_str(&line).unwrap();
    //            all_points.push(SpanSample::parse_jaeger_span(&json_data));
    //        }
    //    }
    //}
    //println!("Parsed n points: {}", all_points.len());

    //let mut map = std::collections::HashMap::new();
    //for item in all_points {
    //    map.entry(item.id.clone()).or_insert(Vec::new()).push(item);
    //}

    //println!("Number of keys: {}", map.len());

    //for (k, v) in map.iter() {
    //    println!("Key: {:?}, Lenght: {}", k, v.len());
    //}

    // Metrics data
    let path = "/home/fsolleza/data/intel/metrics/otlp-metrics-2+0+0000004918.json";
    let lines = {
        let file = File::open(path).unwrap();
        io::BufReader::new(file).lines()
    };
    let mut all_points = Vec::new();
    for line in lines {
        if let Ok(line) = line {
            let json_data: serde_json::Value = serde_json::from_str(&line).unwrap();
            let mut points = MetricSample::parse_otel_resource_metrics(&json_data);
            all_points.append(&mut points);
        }
    }
    println!("Parsed n points: {}", all_points.len());

    let mut map = std::collections::HashMap::new();
    for item in all_points {
        map.entry(item.id.clone()).or_insert(Vec::new()).push(item);
    }

    println!("Number of keys: {}", map.len());

    let mut lengths = Vec::new();
    for (k, v) in map.iter() {
        lengths.push(v.len());
    }
    let min:usize = *lengths.iter().min().unwrap();
    let max:usize = *lengths.iter().max().unwrap();
    let mean:usize = lengths.iter().sum::<usize>() / lengths.len();
    println!("average length {} {} {}", min, max, mean);
}
