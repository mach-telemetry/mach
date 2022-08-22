use otlp::ResourceMetrics;
use std::env;
use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;

#[derive(Hash, Debug, Clone, PartialEq, Eq)]
struct Attributes(Vec<(String, String)>);

#[derive(Hash, Debug, Clone, PartialEq, Eq)]
struct MetricId {
    resource_attribs: Attributes,
    point_attribs: Attributes,
    metric_name: String,
}

#[derive(Debug, Clone)]
struct MetricPoint {
    id: MetricId,
    timestamp: u64,
    value: Vec<f64>,
}

fn parse_attrib_array(attribs: &serde_json::Value) -> Attributes {
    let mut vec = Vec::new();
    for attrib in attribs.as_array().unwrap().iter() {
        vec.push((attrib["key"].as_str().unwrap().into(), attrib["value"]["stringValue"].as_str().unwrap().into()));
    }
    vec.sort();
    Attributes(vec)
}

fn print_keys(json_data: &serde_json::Value) {
    let keys: Vec<&String> = json_data.as_object().unwrap().keys().collect();
    println!("Keys: {:?}", keys);
}

fn parse_resource_metrics(json_data: &serde_json::Value) -> Vec<MetricPoint> {
    let mut all_points = Vec::new();
    for resource_metric in json_data["resourceMetrics"].as_array().unwrap().iter() {
        let resource_attribs = parse_attrib_array(&resource_metric["resource"]["attributes"]);

        for scope in resource_metric["scopeMetrics"].as_array().unwrap().iter() {
            for metric in scope["metrics"].as_array().unwrap().iter() {
                let name = String::from(metric["name"].as_str().unwrap());

                // Gauge
                if !metric["gauge"].is_null() {
                    for point in metric["gauge"]["dataPoints"].as_array().unwrap().iter() {
                        let point_attribs = parse_attrib_array(&point["attributes"]);
                        let metric_id = MetricId {
                            resource_attribs: resource_attribs.clone(),
                            point_attribs,
                            metric_name: name.clone(),
                        };
                        let p = MetricPoint {
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
                        let point_attribs = parse_attrib_array(&point["attributes"]);
                        let metric_id = MetricId {
                            resource_attribs: resource_attribs.clone(),
                            point_attribs,
                            metric_name: name.clone(),
                        };
                        let p = MetricPoint {
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
                        let point_attribs = parse_attrib_array(&point["attributes"]);
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

                        let p = MetricPoint {
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

fn main() {
    println!("Hello world");
    let path = "/home/fsolleza/data/intel/metrics/otlp-metrics-2+0+0000004918.json";
    let lines = {
        let file = File::open(path).unwrap();
        io::BufReader::new(file).lines()
    };
    let mut all_points = Vec::new();
    for line in lines {
        if let Ok(line) = line {
            let json_data: serde_json::Value = serde_json::from_str(&line).unwrap();
            let mut points = parse_resource_metrics(&json_data);
            all_points.append(&mut points);
        }
    }
    println!("Parsed n points: {}", all_points.len());
}
