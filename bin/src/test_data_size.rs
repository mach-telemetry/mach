mod json_telemetry;
use json_telemetry::*;
use std::path::PathBuf;
use std::fs::File;
use std::io::{self, prelude::*, BufRead, BufReader};
use std::path::Path;
use std::collections::HashSet;
use serde_json::*;

fn main() {
    let mut metrics = 0;
    let mut logs = 0;
    let mut spans = 0;
    let statm = procinfo::pid::statm_self().unwrap();
    let rss_before = (statm.resident * 4096) / 1_000_000;
    let samples = DATA.clone();
    for sample in samples.iter() {
        match sample.otel_type {
            OtelType::Metric => metrics += 1,
            OtelType::Log => logs += 1,
            OtelType::Span => spans += 1,
        }
    }
    let statm = procinfo::pid::statm_self().unwrap();
    let rss_after = (statm.resident * 4096) / 1_000_000;
    let mut sources: HashSet<u64> = HashSet::new();
    let mut metrics_sources: HashSet<u64> = HashSet::new();
    let mut logs_sources: HashSet<u64> = HashSet::new();
    let mut spans_sources: HashSet<u64> = HashSet::new();
    samples.iter().for_each(|x| {
        let id = x.source.0;
        sources.insert(id);
        match x.otel_type {
            OtelType::Metric => metrics_sources.insert(id),
            OtelType::Log => logs_sources.insert(id),
            OtelType::Span => spans_sources.insert(id),
        };
    });
    println!("Count: {}", samples.len());
    println!("Number of sources: {}", sources.len());
    println!("Number of metrics sources: {}", metrics_sources.len());
    println!("Number of logs sources: {}", logs_sources.len());
    println!("Number of spans sources: {}", spans_sources.len());
    println!("Metrics: {}", metrics);
    println!("Logs: {}", logs);
    println!("Spans: {}", spans);
    println!("RSS: {}mb", rss_after - rss_before);
}
