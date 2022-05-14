mod otlp;
use std::{
    fs::*,
    io::*,
};
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use mach::utils::random_id;

use otlp::{
    collector::{
        logs::v1::{
            ExportLogsServiceRequest,
            logs_service_client::LogsServiceClient,
        },
        metrics::v1::{
            ExportMetricsServiceRequest,
            metrics_service_client::MetricsServiceClient,
        },
        trace::v1::{
            ExportTraceServiceRequest,
            trace_service_client::TraceServiceClient,
        },
    },
    logs::v1::ResourceLogs,
    metrics::v1::ResourceMetrics,
    trace::v1::ResourceSpans,
    common::v1::{KeyValue, any_value::Value, AnyValue},
    OtlpData,
};
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
use lazy_static::*;
use std::sync::{Arc, atomic::{AtomicUsize, AtomicBool, Ordering::SeqCst}};
use tonic::{transport::Channel, Request};
use tower::timeout::Timeout;

pub fn counter_watcher() {
    let mut samples = [0; 5];
    for i in 0.. {
        std::thread::sleep(Duration::from_secs(1));
        let idx = i % 5;

        let sent = COUNTER.load(SeqCst);
        samples[idx] = sent;
        let max = samples.iter().max().unwrap();
        let min = samples.iter().min().unwrap();
        let sample_rate = (max - min) / 5;

        let queued = QUEUED.load(SeqCst);
        let remaining = queued - sent;

        println!("{} items sent / sec, items enqueued {}", sample_rate, remaining);
        //println!("{} sent, queued {}", sent, queued);
    }
}

lazy_static! {
    pub static ref COUNTER: AtomicUsize = AtomicUsize::new(0);
    pub static ref QUEUED: AtomicUsize = AtomicUsize::new(0);
    pub static ref START_COUNTERS: AtomicBool = AtomicBool::new(false);
    pub static ref ITEMS: Vec<otlp::OtlpData> = {
        println!("Loading");
        let mut file = File::open("/home/fsolleza/data/mach/demo_data2").unwrap();
        let mut data = Vec::new();
        file.read_to_end(&mut data).unwrap();
        let mut items: Vec<otlp::OtlpData> = bincode::deserialize(data.as_slice()).unwrap();
        println!("items read: {}", items.len());
        items
    };
}

async fn logs_client(mut rx: UnboundedReceiver<Vec<ResourceLogs>>) {
    //let channel = Channel::from_static("tcp://0.0.0.0:4317").connect().await.unwrap();
    //let timeout_channel = Timeout::new(channel, Duration::from_micros(1));
    let mut client = LogsServiceClient::connect("http://0.0.0.0:4317").await.unwrap();

    while let Some(resource_logs) = rx.recv().await {
        let to_send = ExportLogsServiceRequest {
            resource_logs,
        };
        match client.export(to_send).await {
            _ => {},
        };
        COUNTER.fetch_add(1, SeqCst);
    }
}

async fn metrics_client(mut rx: UnboundedReceiver<Vec<ResourceMetrics>>) {
    let mut client = MetricsServiceClient::connect("http://0.0.0.0:4317").await.unwrap();

    while let Some(resource_metrics) = rx.recv().await {
        let to_send = ExportMetricsServiceRequest {
            resource_metrics,
        };
        match client.export(to_send).await {
            _ => {},
        };
        COUNTER.fetch_add(1, SeqCst);
    }
}

async fn span_client(mut rx: UnboundedReceiver<Vec<ResourceSpans>>) {
    let mut client = TraceServiceClient::connect("http://0.0.0.0:4317").await.unwrap();

    while let Some(resource_spans) = rx.recv().await {
        let to_send = ExportTraceServiceRequest {
            resource_spans,
        };
        match client.export(to_send).await {
            _ => {},
        };
        COUNTER.fetch_add(1, SeqCst);
    }
}

#[tokio::main]
async fn main() {
    //runner().await;
    let mut handles = Vec::new();
    handles.push(tokio::task::spawn(runner()));
    handles.push(tokio::task::spawn(runner()));
    //handles.push(tokio::task::spawn(runner()));
    for h in handles {
        h.await.unwrap();
    }
}

//#[tokio::main()]
async fn runner() {

    let process_attribute: KeyValue = {
        let value = Some(AnyValue { value: Some(Value::StringValue(random_id())) });
        let key = String::from("loader_id");
        KeyValue {
            key,
            value,
        }
    };

    let mut handles = Vec::new();
    let (logs_tx, logs_rx) = unbounded_channel();
    handles.push(tokio::task::spawn(logs_client(logs_rx)));

    let (metrics_tx, metrics_rx) = unbounded_channel();
    handles.push(tokio::task::spawn(metrics_client(metrics_rx)));

    let (spans_tx, spans_rx) = unbounded_channel();
    handles.push(tokio::task::spawn(span_client(spans_rx)));

    let interval = Duration::from_secs(1) / 1000000;

    //let mut items = ITEMS.clone();
    println!("Loading");
    let mut file = File::open("/home/fsolleza/data/mach/demo_data2").unwrap();
    let mut data = Vec::new();
    file.read_to_end(&mut data).unwrap();
    let mut items: Vec<otlp::OtlpData> = bincode::deserialize(data.as_slice()).unwrap();
    println!("items read: {}", items.len());
    for item in items.iter_mut() {
        item.add_attribute(process_attribute.clone());
    }

    if !START_COUNTERS.swap(true, SeqCst) {
        std::thread::spawn(move || counter_watcher());
    }

    println!("Producing data at: {:?}", interval);
    let mut last = SystemTime::now();
    for mut item in items.drain(..) {
        let nanos: u64 = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos().try_into().unwrap();
        item.update_timestamp(nanos);
        while SystemTime::now().duration_since(last).unwrap() < interval {}
        match item {
            OtlpData::Logs(x) => logs_tx.send(x).unwrap(),
            OtlpData::Metrics(x) => metrics_tx.send(x).unwrap(),
            OtlpData::Spans(x) => spans_tx.send(x).unwrap(),
        }
        QUEUED.fetch_add(1, SeqCst);
        last = SystemTime::now();
    }

    drop(logs_tx);
    drop(metrics_tx);
    drop(spans_tx);

    for h in handles {
        h.await.unwrap()
    }
}
