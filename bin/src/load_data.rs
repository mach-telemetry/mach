mod otlp;
use std::{
    fs::*,
    io::*,
};
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use mach::utils::random_id;
use clap::*;

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
use tokio::sync::mpsc::{Receiver, channel};
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
    pub static ref ITEMS: Vec<OtlpData> = {
        //let mut file = File::open("/home/ubuntu/demo_data2").unwrap();
        let mut file = File::open("/home/fsolleza/data/mach/demo_data2").unwrap();
        let mut data = Vec::new();
        file.read_to_end(&mut data).unwrap();
        let mut items: Vec<otlp::OtlpData> = bincode::deserialize(data.as_slice()).unwrap();
        //items.truncate(1000000);
        items
    };
}

async fn logs_client(mut rx: Receiver<OtlpData>) {
    //let channel = Channel::from_static("tcp://0.0.0.0:4317").connect().await.unwrap();
    //let timeout_channel = Timeout::new(channel, Duration::from_micros(1));
    //let mut client = LogsServiceClient::connect("http://0.0.0.0:4317").await.unwrap();

    //while let Some(mut item) = rx.recv().await {
    //    let nanos: u64 = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos().try_into().unwrap();
    //    item.update_timestamp(nanos);
    //    let resource_logs = match item {
    //        OtlpData::Logs(x) => x,
    //        _ => unreachable!(),
    //    };
    //    let to_send = ExportLogsServiceRequest {
    //        resource_logs,
    //    };
    //    match client.export(to_send).await {
    //        _ => {},
    //    };
    //    COUNTER.fetch_add(1, SeqCst);
    //}
}

async fn metrics_client(mut rx: Receiver<OtlpData>) {
    let mut client = MetricsServiceClient::connect("http://0.0.0.0:4317").await.unwrap();

    while let Some(mut item) = rx.recv().await {
        let nanos: u64 = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos().try_into().unwrap();
        item.update_timestamp(nanos);
        let resource_metrics = match item {
            OtlpData::Metrics(x) => x,
            _ => unreachable!(),
        };
        let to_send = ExportMetricsServiceRequest {
            resource_metrics,
        };
        match client.export(to_send).await {
            _ => {},
        };
        COUNTER.fetch_add(1, SeqCst);
    }
}

async fn span_client(mut rx: Receiver<OtlpData>) {
    let mut client = TraceServiceClient::connect("http://0.0.0.0:4317").await.unwrap();

    while let Some(mut item) = rx.recv().await {
        let nanos: u64 = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos().try_into().unwrap();
        item.update_timestamp(nanos);
        let resource_spans = match item {
            OtlpData::Spans(x) => x,
            _ => unreachable!(),
        };
        let to_send = ExportTraceServiceRequest {
            resource_spans,
        };
        match client.export(to_send).await {
            _ => {},
        };
        COUNTER.fetch_add(1, SeqCst);
    }
}

#[derive(Parser, Debug, Clone)]
struct Args {
    #[clap(short, long, default_value_t = 1)]
    workers: usize,
}


#[tokio::main]
async fn main() {
    let args = Args::parse();
    println!("Args:\n{:#?}", args);
    //runner().await;
    let mut handles = Vec::new();
    for _ in 0..args.workers {
        handles.push(tokio::task::spawn(runner()));
    }
    for h in handles {
        h.await.unwrap();
    }
}

//#[tokio::main()]
async fn runner() {

    let name_id = random_id();
    let process_attribute: KeyValue = {
        let value = Some(AnyValue { value: Some(Value::StringValue(name_id.clone())) });
        let key = String::from("loader_id");
        KeyValue {
            key,
            value,
        }
    };

    let mut handles = Vec::new();
    let (logs_tx, logs_rx) = channel(1);
    handles.push(tokio::task::spawn(logs_client(logs_rx)));

    let (metrics_tx, metrics_rx) = channel(1);
    handles.push(tokio::task::spawn(metrics_client(metrics_rx)));

    let (spans_tx, spans_rx) = channel(1);
    handles.push(tokio::task::spawn(span_client(spans_rx)));

    let interval = Duration::from_secs(1)/1000000;

    //let mut items = ITEMS.clone();
    println!("Loading");
    let mut items = ITEMS.clone();
    //let mut file = File::open("/home/ubuntu/demo_data2").unwrap();
    ////let mut file = File::open("/home/fsolleza/data/mach/demo_data2").unwrap();
    //let mut data = Vec::new();
    //file.read_to_end(&mut data).unwrap();
    //let mut items: Vec<otlp::OtlpData> = bincode::deserialize(data.as_slice()).unwrap();
    //println!("items read: {}", items.len());
    let name_func = |x: &str| -> String {
        format!("{}_{}", x, name_id.as_str())
    };
    for item in items.iter_mut() {
        match item {
            OtlpData::Spans(_) => item.modify_name(name_func),
            OtlpData::Metrics(_) => item.add_attribute(process_attribute.clone()),
            _ => {},
        }
    }

    if !START_COUNTERS.swap(true, SeqCst) {
        std::thread::spawn(move || counter_watcher());
    }

    println!("Producing data at: {:?}", interval);
    let mut last = SystemTime::now();
    loop {
        for item in items.iter() {
            let item = item.clone();
            while SystemTime::now().duration_since(last).unwrap() < interval {}
            match item {

                //OtlpData::Logs(_) => if logs_tx.send(item).await.is_err() {
                //    panic!("Failed to send");
                //} else {
                //    QUEUED.fetch_add(1, SeqCst);
                //},
                OtlpData::Logs(_) => {},

                OtlpData::Metrics(_) => if metrics_tx.send(item).await.is_err() {
                    panic!("Failed to send");
                } else {
                    QUEUED.fetch_add(1, SeqCst);
                },
                OtlpData::Metrics(_) => {},

                OtlpData::Spans(_) => if spans_tx.send(item).await.is_err() {
                    panic!("Failed to send");
                } else {
                    QUEUED.fetch_add(1, SeqCst);
                },
                //OtlpData::Spans(_) => {},

            }
            last = SystemTime::now();
        }
    }

    drop(logs_tx);
    drop(metrics_tx);
    drop(spans_tx);

    for h in handles {
        h.await.unwrap()
    }
}
