mod otlp;
mod tag_index;

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

use tonic::{transport::Server, Request, Response, Status};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender, UnboundedReceiver};
use std::sync::{Arc, RwLock, atomic::{AtomicUsize, Ordering::SeqCst}};
use std::time::Duration;
use std::collections::{HashMap, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use lazy_static::*;
use clap::Parser;
use tag_index::TagIndex;

use mach::{
    durable_queue::{KafkaConfig},
    series::{Types, SeriesConfig},
    compression::{CompressFn, Compression},
    id::SeriesId,
    utils::random_id,
    tsdb::Mach,
    writer::WriterConfig,
    sample::Type,
};

lazy_static! {
    pub static ref QUEUE_LENGTH: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    pub static ref QUEUE_ITEMS: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    pub static ref COUNTER: Arc<AtomicUsize> = {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();
        let mut samples = [0; 5];
        let mut items = [0; 5];
        std::thread::spawn(move || {
            for i in 0.. {
                std::thread::sleep(Duration::from_secs(1));
                let idx = i % 5;

                samples[idx] = counter_clone.load(SeqCst);
                let max = samples.iter().max().unwrap();
                let min = samples.iter().min().unwrap();
                let sample_rate = (max - min) / 5;

                items[idx] = QUEUE_ITEMS.load(SeqCst);
                let max = items.iter().max().unwrap();
                let min = items.iter().min().unwrap();
                let items_rate = (max - min) / 5;

                let ql = QUEUE_LENGTH.load(SeqCst);
                println!("{} samples / sec, {} items / sec, queue length {}", sample_rate, items_rate, ql);
            }
        });
        counter
    };
    pub static ref MACH: Arc<RwLock<Mach>> = Arc::new(RwLock::new(Mach::new()));
    pub static ref TAG_INDEX: TagIndex = TagIndex::new();
}

#[derive(Clone)]
pub struct OtlpServer {
    sender: UnboundedSender<OtlpData>,
}

#[tonic::async_trait]
impl TraceService for OtlpServer {
    async fn export(
        &self,
        req: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        if self.sender.send(OtlpData::Spans(req.into_inner().resource_spans)).is_err() {
            //panic!("Failed to write to sender");
        } else {
            QUEUE_LENGTH.fetch_add(1, SeqCst);
        }
        Ok(Response::new(ExportTraceServiceResponse {}))
    }
}

#[tonic::async_trait]
impl LogsService for OtlpServer {
    async fn export(
        &self,
        req: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        if self.sender.send(OtlpData::Logs(req.into_inner().resource_logs)).is_err() {
            //panic!("Failed to write to sender");
        } else {
            QUEUE_LENGTH.fetch_add(1, SeqCst);
        }
        Ok(Response::new(ExportLogsServiceResponse {}))
    }
}

#[tonic::async_trait]
impl MetricsService for OtlpServer {
    async fn export(
        &self,
        req: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        if self.sender.send(OtlpData::Metrics(req.into_inner().resource_metrics)).is_err() {
            //panic!("Failed to write to sender");
        } else {
            QUEUE_LENGTH.fetch_add(1, SeqCst);
        }
        Ok(Response::new(ExportMetricsServiceResponse {}))
    }
}

fn write_to_file_worker(args: Args, mut receiver: UnboundedReceiver<OtlpData>) {
    let _ = COUNTER.load(SeqCst);
    use std::{
        io::*,
        fs::*,
    };
    let mut file = File::create(args.file_path.as_ref().unwrap()).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let mut buf = Vec::new();
    while let Some(item) = rt.block_on(receiver.recv()) {
        buf.push(item);
        QUEUE_ITEMS.fetch_add(1, SeqCst);
        if buf.len() == args.file_item_count {
            println!("Writing file with items {} items", buf.len());
            let bytes = bincode::serialize(&buf).unwrap();
            file.write_all(bytes.as_slice()).unwrap();
            println!("Done writing");
            panic!("HERE");
            return;
        }
    }
}

fn kafka_worker(args: Args, mut receiver: UnboundedReceiver<OtlpData>) {
    use rdkafka::{
        admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
        client::DefaultClientContext,
        config::ClientConfig,
        producer::{FutureProducer, FutureRecord},
    };
    use zstd::stream::{encode_all};

    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();

    let client: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &args.kafka_bootstraps)
        .create()
        .unwrap();
    let admin_opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(3)));
    let topics = &[NewTopic {
        name: &args.kafka_topic,
        num_partitions: args.kafka_partitions,
        replication: TopicReplication::Fixed(args.kafka_replication),
        config: Vec::new(),
    }];
    rt.block_on(client.create_topics(topics, &admin_opts)).unwrap();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", args.kafka_bootstraps)
        .set("acks", &args.kafka_acks)
        .set("message.max.bytes", "1000000000")
        .set("linger.ms", "0")
        .set("message.copy.max.bytes", "5000000")
        .set("message.timeout.ms", "3000")
        .set("compression.type", "none")
        .create()
        .unwrap();

    let mut buf = Vec::new();
    while let Some(item) = rt.block_on(receiver.recv()) {
        let _c = COUNTER.load(SeqCst);
        QUEUE_LENGTH.fetch_sub(1, SeqCst);
        buf.push(item);
        if buf.len() == args.kafka_batch {
            let encoded = bincode::serialize(&buf).unwrap();
            let bytes = encode_all(encoded.as_slice(), 0).unwrap();
            let to_send: FutureRecord<str, [u8]> =
                FutureRecord::to(&args.kafka_topic).payload(&bytes);
            match rt.block_on(producer.send(to_send, Duration::from_secs(3))) {
                Ok(_) => QUEUE_ITEMS.fetch_add(args.kafka_batch, SeqCst),
                Err(_) => panic!("Producer failed"),
            };
            buf.clear();
        }
    }
}

fn mach_worker(args: Args, mut receiver: UnboundedReceiver<OtlpData>) {
    let queue_config = KafkaConfig {
        bootstrap: args.kafka_bootstraps.clone(),
        topic: args.kafka_topic.clone(),
    }
    .config();
    let writer_config = WriterConfig {
        queue_config,
        active_block_flush_sz: 1_000_000,
    };
    let mut writer = (*MACH).write().unwrap().add_writer(writer_config).unwrap();
    let mut id_map = HashMap::new();

    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();

    while let Some(item) = rt.block_on(receiver.recv()) {
        QUEUE_LENGTH.fetch_sub(1, SeqCst);
        let mut count = 0;
        match item {
            OtlpData::Metrics(resource_metrics) => {

                // Iterate over each resource
                for resource in &resource_metrics {
                    let resource_attribs = &resource.resource.as_ref().unwrap().attributes;

                    // Iterate over each scope
                    for scope in &resource.scope_metrics {
                        let scope_name = &scope.scope.as_ref().unwrap().name;

                        // Iterate over each metric
                        for metric in &scope.metrics {
                            let metric_name = &metric.name;

                            // There are several types of metrics
                            match metric.data.as_ref().unwrap() {
                                Data::Gauge(x) => {

                                    // Iterate over each point
                                    for point in &x.data_points {
                                        let point_attribs = &point.attributes;
                                        let mut hasher = DefaultHasher::new();
                                        resource_attribs.hash(&mut hasher);
                                        scope_name.hash(&mut hasher);
                                        metric_name.hash(&mut hasher);
                                        point_attribs.hash(&mut hasher);
                                        let id = SeriesId(hasher.finish());

                                        // Get reference, if no reference, register source
                                        let _id_ref = *id_map.entry(id).or_insert_with(|| {
                                            let types = vec![Types::F64];
                                            let compression = Compression::from(
                                                vec![CompressFn::Decimal(3)]
                                            );

                                            let conf = SeriesConfig {
                                                id,
                                                types,
                                                compression,
                                                seg_count: 3,
                                                nvars: 1,
                                            };

                                            (*MACH).write().unwrap().add_series(conf).unwrap();
                                            let id_ref = writer.get_reference(id);
                                            id_ref
                                        });

                                        unimplemented!();
                                        //count += 1;
                                    }
                                },

                                Data::Sum(x) => {
                                    for point in &x.data_points {
                                        let point_attribs = &point.attributes;
                                        let mut hasher = DefaultHasher::new();
                                        resource_attribs.hash(&mut hasher);
                                        scope_name.hash(&mut hasher);
                                        metric_name.hash(&mut hasher);
                                        point_attribs.hash(&mut hasher);
                                        let id = SeriesId(hasher.finish());

                                        // Get reference, if no reference, register source
                                        let id_ref = *id_map.entry(id).or_insert_with(|| {
                                            let types = vec![Types::F64];
                                            let compression = Compression::from(
                                                vec![CompressFn::Decimal(3)]
                                            );

                                            let conf = SeriesConfig {
                                                id,
                                                types,
                                                compression,
                                                seg_count: 3,
                                                nvars: 1,
                                            };

                                            (*MACH).write().unwrap().add_series(conf).unwrap();
                                            let id_ref = writer.get_reference(id);
                                            id_ref
                                        });

                                        // Push sample
                                        let timestamp = point.time_unix_nano;
                                        let value = match point.value.as_ref().unwrap() {
                                            number_data_point::Value::AsDouble(x) => *x,
                                            number_data_point::Value::AsInt(x) => {
                                                let x: i32 = (*x).try_into().unwrap();
                                                x.into()
                                            }
                                        };
                                        let value = &[Type::F64(value)];
                                        loop {
                                            if writer.push(id_ref, timestamp, value).is_ok() {
                                                break;
                                            }
                                        }
                                        count += 1;
                                    }
                                },

                                Data::Histogram(x) => {
                                    for point in &x.data_points {
                                        let point_attribs = &point.attributes;
                                        let mut hasher = DefaultHasher::new();
                                        resource_attribs.hash(&mut hasher);
                                        scope_name.hash(&mut hasher);
                                        metric_name.hash(&mut hasher);
                                        point_attribs.hash(&mut hasher);
                                        let id = SeriesId(hasher.finish());

                                        // Get reference, if no reference, register source
                                        let id_ref = *id_map.entry(id).or_insert_with(|| {
                                            let nvars = point.bucket_counts.len();
                                            let types = vec![Types::F64; nvars];
                                            let compression = Compression::from(
                                                vec![CompressFn::Decimal(3); nvars]
                                            );

                                            let conf = SeriesConfig {
                                                id,
                                                types,
                                                compression,
                                                seg_count: 3,
                                                nvars,
                                            };

                                            (*MACH).write().unwrap().add_series(conf).unwrap();
                                            let id_ref = writer.get_reference(id);
                                            id_ref
                                        });

                                        // Push sample
                                        let timestamp = point.time_unix_nano;
                                        let value: Vec<Type> = point.bucket_counts.iter().map(|x| {
                                            let x: i32 = (*x).try_into().unwrap();
                                            Type::F64(x.into())
                                        }).collect();

                                        loop {
                                            if writer.push(id_ref, timestamp, value.as_slice()).is_ok() {
                                                break;
                                            }
                                        }
                                        count += 1;
                                    }
                                },

                                Data::ExponentialHistogram(x) => {
                                    for point in &x.data_points {
                                        let point_attribs = &point.attributes;
                                        let mut hasher = DefaultHasher::new();
                                        resource_attribs.hash(&mut hasher);
                                        scope_name.hash(&mut hasher);
                                        metric_name.hash(&mut hasher);
                                        point_attribs.hash(&mut hasher);
                                        let id = SeriesId(hasher.finish());

                                        // Get reference, if no reference, register source
                                        let _id_ref = *id_map.entry(id).or_insert_with(|| {
                                            let types = vec![Types::F64];
                                            let compression = Compression::from(
                                                vec![CompressFn::Decimal(3)]
                                            );

                                            let conf = SeriesConfig {
                                                id,
                                                types,
                                                compression,
                                                seg_count: 3,
                                                nvars: 1,
                                            };

                                            (*MACH).write().unwrap().add_series(conf).unwrap();
                                            let id_ref = writer.get_reference(id);
                                            id_ref
                                        });
                                        //count += 1;
                                        unimplemented!();
                                    }
                                },

                                Data::Summary(x) => {
                                    for point in &x.data_points {
                                        let point_attribs = &point.attributes;
                                        let mut hasher = DefaultHasher::new();
                                        resource_attribs.hash(&mut hasher);
                                        scope_name.hash(&mut hasher);
                                        metric_name.hash(&mut hasher);
                                        point_attribs.hash(&mut hasher);
                                        let id = SeriesId(hasher.finish());

                                        // Get reference, if no reference, register source
                                        let _id_ref = *id_map.entry(id).or_insert_with(|| {
                                            let types = vec![Types::F64];
                                            let compression = Compression::from(
                                                vec![CompressFn::Decimal(3)]
                                            );

                                            let conf = SeriesConfig {
                                                id,
                                                types,
                                                compression,
                                                seg_count: 3,
                                                nvars: 1,
                                            };

                                            (*MACH).write().unwrap().add_series(conf).unwrap();
                                            let id_ref = writer.get_reference(id);
                                            id_ref
                                        });
                                        //count += 1;
                                        unimplemented!();
                                    }
                                },

                            } // match brace
                        } // metric loop
                    } // scope loop
                } // resource loop
            }, // match Metric

            OtlpData::Logs(resource_logs) => {

                // Iterate over each resource
                for resource in &resource_logs {
                    let resource_attribs = &resource.resource.as_ref().unwrap().attributes;

                    // Iterate over each scope
                    for scope in &resource.scope_logs {
                        let scope_name = &scope.scope.as_ref().unwrap().name;

                        // Iterate over each log
                        for log in &scope.log_records {
                            let log_attribs = &log.attributes;
                            let mut hasher = DefaultHasher::new();
                            resource_attribs.hash(&mut hasher);
                            scope_name.hash(&mut hasher);
                            log_attribs.hash(&mut hasher);
                            let id = SeriesId(hasher.finish());

                            // Get reference, if no reference, register source
                            let id_ref = *id_map.entry(id).or_insert_with(|| {
                                let types = vec![ Types::Bytes, ];
                                let compression = Compression::from(
                                    vec![CompressFn::BytesLZ4]
                                );
                                let nvars = types.len();

                                let conf = SeriesConfig {
                                    id,
                                    types,
                                    compression,
                                    seg_count: 3,
                                    nvars,
                                };

                                (*MACH).write().unwrap().add_series(conf).unwrap();
                                let id_ref = writer.get_reference(id);
                                id_ref
                            });

                            // Push span
                            let timestamp = log.time_unix_nano;
                            let bytes = &[
                                Type::Bytes(bincode::serialize(&log).unwrap()),
                            ];
                            loop {
                                if writer.push(id_ref, timestamp, bytes).is_ok() {
                                    break;
                                }
                            }
                            count += 1;
                        } // log loop
                    } // scope loop
                } // resource loop
            }, // match Logs

            OtlpData::Spans(resource_spans) => {

                // Iterate over each resource
                for resource in resource_spans {
                    let resource_attribs = &resource.resource.as_ref().unwrap().attributes;

                    // Iterate over each scope
                    for scope in resource.scope_spans {
                        let scope_name = &scope.scope.as_ref().unwrap().name;

                        // Iterate over each span
                        for span in scope.spans {
                            let mut hasher = DefaultHasher::new();
                            resource_attribs.hash(&mut hasher);
                            scope_name.hash(&mut hasher);
                            span.attributes.hash(&mut hasher);
                            let id = SeriesId(hasher.finish());

                            // Get reference, if no reference, register source
                            let id_ref = *id_map.entry(id).or_insert_with(|| {
                                let types = vec![ 
                                    Types::Bytes,
                                    Types::Bytes,
                                    Types::Bytes,
                                ];
                                let compression = Compression::from(
                                    vec![
                                        CompressFn::BytesLZ4,
                                        CompressFn::BytesLZ4,
                                        CompressFn::BytesLZ4,
                                    ]
                                );
                                let nvars = types.len();

                                let conf = SeriesConfig {
                                    id,
                                    types,
                                    compression,
                                    seg_count: 3,
                                    nvars,
                                };

                                (*MACH).write().unwrap().add_series(conf).unwrap();
                                let id_ref = writer.get_reference(id);
                                id_ref
                            });

                            // Push span
                            let timestamp = span.start_time_unix_nano;
                            let trace_id = Type::Bytes(span.trace_id);
                            let span_id = Type::Bytes(span.span_id);
                            let parent_id = Type::Bytes(span.parent_span_id);

                            //let bytes = Type::Bytes(bincode::serialize(&span).unwrap());
                            let slice = &[ trace_id, span_id, parent_id ];
                            loop {
                                if writer.push(id_ref, timestamp, slice).is_ok() {
                                    break;
                                }
                            }
                            count += 1;
                        } // span loop
                    } // scope loop
                } // resource loop
            }, // match Span
        } // match item

        COUNTER.fetch_add(count, SeqCst);
        QUEUE_ITEMS.fetch_add(1, SeqCst);
    }
}

#[derive(Parser, Debug, Clone)]
struct Args {
    #[clap(short, long, default_value_t = String::from("mach"), parse(try_from_str=validate_tsdb))]
    tsdb: String,

    #[clap(short, long, default_value_t = String::from("0.0.0.0"))]
    server_addr: String,

    #[clap(short, long, default_value_t = String::from("4317"))]
    server_port: String,

    #[clap(short, long, default_value_t = String::from("localhost:9093,localhost:9094,localhost:9095"))]
    kafka_bootstraps: String,

    #[clap(short, long, default_value_t = random_id())]
    kafka_topic: String,

    #[clap(short, long, default_value_t = 3)]
    kafka_partitions: i32,

    #[clap(short, long, default_value_t = 3)]
    kafka_replication: i32,

    #[clap(short, long, default_value_t = String::from("all"), parse(try_from_str=validate_ack))]
    kafka_acks: String,

    #[clap(short, long, default_value_t = 8192)]
    kafka_batch: usize,

    #[clap(short, long)]
    file_path: Option<String>,

    #[clap(short, long, default_value_t = 1000000)]
    file_item_count: usize,

}

fn validate_tsdb(s: &str) -> Result<String, String> {
    Ok(match s {
        "mach" => s.into(),
        "kafka" => s.into(),
        "file" => s.into(),
        _ => return Err(format!("Invalid option {}, valid options are \"mach\", \"kafka\".", s))
    })
}

fn validate_ack(s: &str) -> Result<String, String> {
    Ok(match s {
        "all" => s.into(),
        "1" => s.into(),
        "0" => s.into(),
        "2" => s.into(),
        _ => return Err("Valid options are: all, \"1\", \"2\", \"3\"".into())
    })
}

#[tokio::main]
async fn main()  -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    println!("Args: {:#?}", args);
    let mut addr = String::from(args.server_addr.as_str());
    addr.push(':');
    addr.push_str(args.server_port.as_str());
    let addr = addr.parse().unwrap();

    let (tx, rx) = unbounded_channel();
    let args_clone = args.clone();

    match args.tsdb.as_str() {
        "mach" => std::thread::spawn(move || { mach_worker(args_clone, rx) }),
        "kafka" => std::thread::spawn(move || { kafka_worker(args_clone, rx) }),
        "file" => std::thread::spawn(move || { write_to_file_worker(args_clone, rx) }),
        _ => unreachable!(), // validation covers this
    };

    let server = OtlpServer { sender: tx };
    Server::builder()
        .add_service(TraceServiceServer::new( server.clone() ))
        .add_service(LogsServiceServer::new( server.clone() ))
        .add_service(MetricsServiceServer::new( server.clone() ))
        .serve(addr)
        .await?;
    Ok(())
}
