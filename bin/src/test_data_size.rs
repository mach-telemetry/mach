// Copyright (c) 2023 Franco Solleza, Intel Corporation, Brown University
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

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
