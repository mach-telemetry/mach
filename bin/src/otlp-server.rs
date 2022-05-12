mod otlp;

use otlp::{
    logs::v1::ResourceLogs,
    metrics::v1::{ResourceMetrics, metric::Data, number_data_point},
    trace::v1::ResourceSpans,
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
};

use tonic::{transport::Server, Request, Response, Status};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender, UnboundedReceiver};
use std::sync::{Arc, RwLock, atomic::{AtomicUsize, Ordering::SeqCst}};
use std::time::Duration;
use std::collections::{HashMap, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use lazy_static::*;
use clap::Parser;

use mach::{
    durable_queue::{KafkaConfig},
    series::{Types, SeriesConfig},
    compression::{CompressFn, Compression},
    id::SeriesId,
    utils::{random_id, bytes::Bytes},
    tsdb::Mach,
    writer::WriterConfig,
    sample::Type,
};

lazy_static! {
    pub static ref COUNTER: Arc<AtomicUsize> = {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();
        let mut data = [0; 5];
        tokio::spawn(async move {
            for i in 0.. {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let idx = i % 5;
                data[idx] = counter_clone.load(SeqCst);
                let max = data.iter().max().unwrap();
                let min = data.iter().min().unwrap();
                let rate = (max - min) / 5;
                println!("received {} / sec", rate);
            }
        });
        counter
    };

    pub static ref SINK: Arc<AtomicUsize> = {
        let sink = Arc::new(AtomicUsize::new(0));
        let clone = sink.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(3)).await;
                println!("SINK: {}", clone.load(SeqCst));
            }
        });
        sink
    };

    pub static ref MACH: Arc<RwLock<Mach>> = Arc::new(RwLock::new(Mach::new()));
}

pub enum OtlpData {
    Logs(Vec<ResourceLogs>),
    Metrics(Vec<ResourceMetrics>),
    Spans(Vec<ResourceSpans>),
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
            panic!("BLAH");
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
            panic!("BLAH");
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
            panic!("BLAH");
        }
        Ok(Response::new(ExportMetricsServiceResponse {}))
    }
}

async fn otlp_worker(mut receiver: UnboundedReceiver<OtlpData>, bootstraps: String) {
    let queue_config = KafkaConfig {
        bootstrap: bootstraps.into(),
        topic: random_id(),
    }
    .config();
    let writer_config = WriterConfig {
        queue_config,
        active_block_flush_sz: 1_000_000,
    };
    let mut writer = (*MACH).write().unwrap().add_writer(writer_config).unwrap();
    let mut id_map = HashMap::new();

    while let Some(item) = receiver.recv().await {

        let mut count = 0;

        match item {
            OtlpData::Metrics(resource_metrics) => {

                // Iterate over each resource
                for resource in &resource_metrics {
                    let resource_attribs = &resource.resource.as_ref().unwrap().attributes;
                        let schema_url = &resource.schema_url;

                    // Iterate over each scope
                    for scope in &resource.scope_metrics {
                        let scope_name = &scope.scope.as_ref().unwrap().name;
                        let schema_url = &scope.schema_url;

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
                                        count += 1;
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
                                        count += 1;
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
                                        count += 1;
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
                                let types = vec![
                                    Types::Bytes,
                                ];
                                let compression = Compression::from(
                                    vec![CompressFn::BytesLZ4]
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
                            unimplemented!();
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
                        for mut span in scope.spans {
                            let mut hasher = DefaultHasher::new();
                            resource_attribs.hash(&mut hasher);
                            scope_name.hash(&mut hasher);
                            span.attributes.hash(&mut hasher);
                            let id = SeriesId(hasher.finish());

                            // Get reference, if no reference, register source
                            let id_ref = *id_map.entry(id).or_insert_with(|| {
                                let types = vec![
                                    Types::Bytes, // TraceId
                                    //Types::Bytes, // SpanId
                                    //Types::Bytes, // Parent SpanId
                                    //Types::Bytes, // Name
                                    //Types::F64,   // Duration
                                    //Types::Bytes, // Events
                                    //Types::Bytes, // Links
                                ];
                                let compression = Compression::from(
                                    vec![
                                        CompressFn::BytesLZ4,
                                        //CompressFn::BytesLZ4,
                                        //CompressFn::BytesLZ4,
                                        //CompressFn::BytesLZ4,
                                        //CompressFn::Decimal(3),
                                        //CompressFn::BytesLZ4,
                                        //CompressFn::BytesLZ4,
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
                            //let traceid = Type::Bytes(span.trace_id);
                            //let spanid = Type::Bytes(span.span_id);
                            //let parentid = Type::Bytes(span.parent_span_id);
                            //let name = Type::Bytes(span.name.into_bytes());
                            //let duration = Type::F64({
                            //    let d: u32 = (span.end_time_unix_nano - timestamp).try_into().unwrap();
                            //    d.into()
                            //});
                            //let events = Type::Bytes(bincode::serialize(&span.events).unwrap());
                            //let links = Type::Bytes(bincode::serialize(&span.links).unwrap());
                            let bytes = Type::Bytes(bincode::serialize(&span).unwrap());

                            let slice = &[
                                //traceid,
                                //spanid,
                                //parentid,
                                //name,
                                //duration,
                                //events,
                                //links
                                bytes
                            ];
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
    }
}

#[derive(Parser, Debug)]
struct Args {
    #[clap(short, long, default_value_t = String::from("mach"))]
    tsdb: String,

    #[clap(short, long, default_value_t = String::from("0.0.0.0"))]
    addr: String,

    #[clap(short, long, default_value_t = String::from("4317"))]
    port: String,

    #[clap(long)]
    path: Option<String>,

    #[clap(short, long, default_value_t = String::from("localhost:9093,localhost:9094,localhost:9095"))]
    kafka: String,
}

#[tokio::main]
async fn main()  -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    println!("Args: {:?}", args);
    let mut addr = String::from(args.addr.as_str());
    addr.push(':');
    addr.push_str(args.port.as_str());
    let addr = addr.parse().unwrap();

    let kafka = args.kafka.clone();

    let (tx, rx) = unbounded_channel();
    tokio::task::spawn(otlp_worker(rx, kafka));

    let server = OtlpServer { sender: tx };
    Server::builder()
        .add_service(TraceServiceServer::new( server.clone() ))
        .add_service(LogsServiceServer::new( server.clone() ))
        .add_service(MetricsServiceServer::new( server.clone() ))
        .serve(addr)
        .await?;
    Ok(())
}
