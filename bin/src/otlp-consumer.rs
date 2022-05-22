mod otlp;

use clap::*;
use rdkafka::{
    config::ClientConfig,
    consumer::{stream_consumer::StreamConsumer, CommitMode, Consumer, DefaultConsumerContext},
    Message,
};
use mach::{
    utils::random_id,
    active_block::StaticBlock,
};
use zstd::stream::decode_all;
use std::time::{UNIX_EPOCH, Duration, SystemTime};
use otlp::{
    metrics::v1::{metric::Data, number_data_point},
    collector::{
        logs::v1::{
            logs_service_server::{LogsService, LogsServiceServer},
            ExportLogsServiceRequest, ExportLogsServiceResponse,
        },
        metrics::v1::{
            metrics_service_server::{MetricsService, MetricsServiceServer},
            ExportMetricsServiceRequest, ExportMetricsServiceResponse,
        },
        trace::v1::{
            trace_service_server::{TraceService, TraceServiceServer},
            ExportTraceServiceRequest, ExportTraceServiceResponse,
        },
    },
    OtlpData,
};


async fn mach_consumer(args: Args) {
    let consumer: StreamConsumer<DefaultConsumerContext> = ClientConfig::new()
        .set("bootstrap.servers", &args.kafka_bootstraps)
        .set("group.id", random_id())
        .create()
        .unwrap();
    consumer
        .subscribe(&[&args.kafka_topic])
        .expect("Can't subscribe to specified topics");

    println!("Reading data");

    loop {
        match consumer.recv().await {
            Err(e) => println!("Kafka error: {}", e),
            Ok(m) => {
                match m.payload_view::<[u8]>() {
                    None => {}
                    Some(Ok(s)) => {
                        let sz = s.len();
                        // we use zstd inside mach to compress the block before writing to kafka
                        let block = StaticBlock::new(decode_all(s).unwrap());

                        //let block = StaticBlock::new(s);
                        let count = block.samples();
                        let max_ts = block.max_timestamp();
                        let now = SystemTime::now();
                        let time = UNIX_EPOCH + Duration::from_nanos(max_ts);
                        let gap = now.duration_since(time).unwrap();
                        println!("Block size: {}, sample count: {}, gap: {:?}", sz, count, gap);
                    }
                    Some(Err(_)) => {
                        println!("Error while deserializing message payload");
                    }
                };
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        }
    }
}

async fn kafka_consumer(args: Args) {
    let consumer: StreamConsumer<DefaultConsumerContext> = ClientConfig::new()
        .set("bootstrap.servers", &args.kafka_bootstraps)
        .set("group.id", random_id())
        .create()
        .unwrap();
    consumer
        .subscribe(&[&args.kafka_topic])
        .expect("Can't subscribe to specified topics");

    println!("Reading data");

    loop {
        match consumer.recv().await {
            Err(e) => println!("Kafka error: {}", e),
            Ok(m) => {
                match m.payload_view::<[u8]>() {
                    None => {}
                    Some(Ok(s)) => {
                        let sz = s.len();
                        let bytes = decode_all(s).unwrap();
                        let uncompressed_sz = bytes.len();
                        let mut data = Vec::new();
                        let mut slice = &bytes[..];
                        while slice.len() > 0 {
                            let sz = usize::from_be_bytes(slice[..8].try_into().unwrap());
                            let item = bincode::deserialize(&slice[8..8+sz]).unwrap();
                            data.push(item);
                            slice = &slice[sz + 8..];
                        }
                        //let data: Vec<OtlpData> = bincode::deserialize(decode_all(s).unwrap().as_slice()).unwrap();
                        let mut count = 0;
                        let mut max_ts = 0;
                        for item in data.iter() {
                            let (c, m) = get_count_max_time(&item);
                            count += c;
                            max_ts = max_ts.max(m);
                        }
                        let now = SystemTime::now();
                        let time = UNIX_EPOCH + Duration::from_nanos(max_ts);
                        let gap = now.duration_since(time).unwrap();
                        println!("Block size: {}, uncomp: {}, sample count: {}, gap: {:?}", sz, uncompressed_sz, count, gap);
                    }
                    Some(Err(_)) => {
                        println!("Error while deserializing message payload");
                    }
                };
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        }
    }
}

fn get_count_max_time(data: &OtlpData) -> (usize, u64) {
    let mut count = 0;
    let mut max_time = 0;

    match data {
        OtlpData::Metrics(resource_metrics) => {
            for resource in resource_metrics.iter() {
                for scope in resource.scope_metrics.iter() {
                    for metric in scope.metrics.iter() {
                        match metric.data.as_ref().unwrap() {
                            Data::Gauge(x) => {
                                // Iterate over each point
                                for point in x.data_points.iter() {
                                    max_time = max_time.max(point.time_unix_nano);
                                    count += 1;
                                }
                            },

                            Data::Sum(x) => {
                                for point in x.data_points.iter() {
                                    max_time = max_time.max(point.time_unix_nano);
                                    count += 1;
                                }
                            },

                            Data::Histogram(x) => {
                                for point in x.data_points.iter() {
                                    max_time = max_time.max(point.time_unix_nano);
                                    count += 1;
                                }
                            },

                            Data::ExponentialHistogram(x) => {
                                for point in x.data_points.iter() {
                                    max_time = max_time.max(point.time_unix_nano);
                                    count += 1;
                                }
                            },

                            Data::Summary(x) => {
                                for point in x.data_points.iter() {
                                    max_time = max_time.max(point.time_unix_nano);
                                    count += 1;
                                }
                            },

                        } // match brace
                    } // metric loop
                } // scope loop
            } // resource loop
        }, // match Metric

        OtlpData::Logs(resource_logs) => {
            for resource in resource_logs.iter() {
                for scope in resource.scope_logs.iter() {
                    for log in scope.log_records.iter() {
                        max_time = max_time.max(log.time_unix_nano);
                        count += 1;
                    } // log loop
                } // scope loop
            } // resource loop
        }, // match Logs

        OtlpData::Spans(resource_spans) => {
            for resource in resource_spans.iter() {
                for scope in resource.scope_spans.iter() {
                    for span in scope.spans.iter() {
                        max_time = max_time.max(span.end_time_unix_nano);
                        count += 1;
                    } // span loop
                } // scope loop
            } // resource loop
        }, // match Span
    } // match item

    (count, max_time)
}

#[derive(Parser, Debug, Clone)]
struct Args {

    #[clap(short, long, default_value_t = String::from("mach"), parse(try_from_str=validate_tsdb))]
    tsdb: String,

    #[clap(short, long, default_value_t = String::from("localhost:9093,localhost:9094,localhost:9095"))]
    kafka_bootstraps: String,

    #[clap(short, long)]
    kafka_topic: String,
}

fn validate_tsdb(s: &str) -> Result<String, String> {
    Ok(match s {
        "mach" => s.into(),
        "kafka" => s.into(),
        _ => return Err(format!("Invalid option {}, valid options are \"mach\", \"kafka\".", s))
    })
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    println!("Args: {:#?}", args);
    match args.tsdb.as_str() {
        "mach" => mach_consumer(args).await,
        "kafka" => kafka_consumer(args).await,
        _ => unreachable!(), // covered by validate
    }
}
