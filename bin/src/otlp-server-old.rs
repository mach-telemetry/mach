mod otlp;
mod tag_index;
mod id_index;

use otlp::{
    metrics::v1::{metric::Data, MetricsData, number_data_point, ResourceMetrics},
    logs::v1::{LogsData},
    trace::v1::{TracesData, ResourceSpans, Span},
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
use prost::Message;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender, UnboundedReceiver};
use std::sync::{Arc, RwLock, atomic::{AtomicUsize, Ordering::SeqCst}};
use std::sync::mpsc;
use std::time::Duration;
use std::collections::{HashMap, HashSet, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use lazy_static::*;
use clap::Parser;
use tag_index::TagIndex;

use mach::{
    durable_queue::{KafkaConfig},
    series::{Types, SeriesConfig},
    compression::{CompressFn, Compression},
    id::{WriterId, SeriesId, SeriesRef},
    utils::random_id,
    tsdb::Mach,
    writer::WriterConfig,
    sample::Type,
};

fn init_counters() {
    let mut samples = [0; 5];
    let mut items = [0; 5];
    std::thread::spawn(move || {
        for i in 0.. {
            std::thread::sleep(Duration::from_secs(1));
            let idx = i % 5;

            samples[idx] = SAMPLES.load(SeqCst);
            let max = samples.iter().max().unwrap();
            let min = samples.iter().min().unwrap();
            let sample_rate = (max - min) / 5;

            items[idx] =ITEMS.load(SeqCst);
            let max = items.iter().max().unwrap();
            let min = items.iter().min().unwrap();
            let items_rate = (max - min) / 5;

            let ql = QUEUE_LENGTH.load(SeqCst);
            println!("{} samples / sec, {} items / sec, queue length {}", sample_rate, items_rate, ql);
        }
    });
}

lazy_static! {
    pub static ref QUEUE_LENGTH: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    pub static ref ITEMS: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    pub static ref SAMPLES: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    pub static ref COUNTER_INIT: bool = {
        init_counters();
        true
    };
    pub static ref MACH: Arc<RwLock<Mach>> = Arc::new(RwLock::new(Mach::new()));
    pub static ref TAG_INDEX: TagIndex = TagIndex::new();
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

    #[clap(short, long, default_value_t = 1)]
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

    #[clap(short, long, default_value_t = 1000000)]
    mach_active_block_sz: usize,
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

#[derive(Clone)]
pub struct KafkaServer {
    sender: UnboundedSender<(usize, Vec<u8>)>,
}

#[tonic::async_trait]
impl TraceService for KafkaServer {
    async fn export(
        &self,
        req: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let data = OtlpData::Spans(req.into_inner().resource_spans);
        let sample_count = data.sample_count();
        let bytes = bincode::serialize(&data).unwrap();

        if self.sender.send((sample_count, bytes)).is_err() {
            panic!("Failed to write to sender");
        } else {
            QUEUE_LENGTH.fetch_add(1, SeqCst);
        }
        Ok(Response::new(ExportTraceServiceResponse {}))
    }
}

#[tonic::async_trait]
impl LogsService for KafkaServer {
    async fn export(
        &self,
        req: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        unimplemented!()
    }
}

#[tonic::async_trait]
impl MetricsService for KafkaServer {
    async fn export(
        &self,
        req: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        unimplemented!();
        //if self.sender.send(OtlpData::Metrics(req.into_inner().resource_metrics)).is_err() {
        //    panic!("Failed to write to sender");
        //} else {
        //    QUEUE_LENGTH.fetch_add(1, SeqCst);
        //}
        //Ok(Response::new(ExportMetricsServiceResponse {}))
    }
}

fn kafka_worker(args: Args, mut receiver: UnboundedReceiver<(usize, Vec<u8>)>) {
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
        //.set("message.max.bytes", "1000000000")
        .set("linger.ms", "0")
        //.set("message.copy.max.bytes", "1000000000")
        //.set("message.timeout.ms", "3000")
        .set("compression.type", "none")
        .create()
        .unwrap();

    //let mut byte_buffer: Vec<u8> = Vec::with_capacity(2usize.pow(15));
    let mut buf = Vec::new();
    while let Some((samples, item)) = rt.block_on(receiver.recv()) {
        let _ = *COUNTER_INIT;
        QUEUE_LENGTH.fetch_sub(1, SeqCst);

        let len = buf.len();
        buf.extend_from_slice(&item.len().to_be_bytes());
        buf.extend_from_slice(item.as_slice());
        ITEMS.fetch_add(1, SeqCst);
        if buf.len() > 1000000 {
            let bytes = encode_all(&buf[..], 0).unwrap();
            let to_send: FutureRecord<str, [u8]> =
                FutureRecord::to(&args.kafka_topic).payload(&bytes);
            match rt.block_on(producer.send(to_send, Duration::from_secs(3))) {
                Ok(x) => {},
                Err(_) => panic!("Producer failed"),
            };
            buf.clear();
        }
        SAMPLES.fetch_add(samples, SeqCst);
    }
}

fn write_to_file_worker(args: Args, mut receiver: UnboundedReceiver<OtlpData>) {
    let _ = SAMPLES.load(SeqCst);
    use std::{
        io::*,
        fs::*,
    };
    let mut file = File::create(args.file_path.as_ref().unwrap()).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let mut buf = Vec::new();
    while let Some(item) = rt.block_on(receiver.recv()) {
        buf.push(item);
       ITEMS.fetch_add(1, SeqCst);
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


#[derive(Clone)]
pub struct MachServer {
    writer: UnboundedSender<Vec<(SeriesId, u64, Vec<Type>)>>,
}

#[tonic::async_trait]
impl TraceService for MachServer {
    async fn export(
        &self,
        req: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let spans = get_span_samples(req.into_inner().resource_spans);
        let count = spans.len();
        ITEMS.fetch_add(1, SeqCst);
        if self.writer.send(spans).is_err() {
            panic!("Failed to write to sender");
        } else {
            QUEUE_LENGTH.fetch_add(count, SeqCst);
        }
        Ok(Response::new(ExportTraceServiceResponse {}))
    }
}

#[tonic::async_trait]
impl LogsService for MachServer {
    async fn export(
        &self,
        req: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        unimplemented!()
    }
}

#[tonic::async_trait]
impl MetricsService for MachServer {
    async fn export(
        &self,
        req: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let metrics = get_metrics_samples(req.into_inner().resource_metrics);
        let count = metrics.len();
        ITEMS.fetch_add(1, SeqCst);
        if self.writer.send(metrics).is_err() {
            panic!("Failed to write to sender");
        } else {
            QUEUE_LENGTH.fetch_add(count, SeqCst);
        }
        Ok(Response::new(ExportMetricsServiceResponse {}))
    }
}

fn get_series_config(id: SeriesId, values: &[Type]) -> SeriesConfig {
    let mut types = Vec::new();
    let mut compression = Vec::new();
    values.iter().for_each(|v| {
        let (t, c) = match v {
            Type::U32(_) => (Types::U32, CompressFn::IntBitpack),
            Type::U64(_) => (Types::U64, CompressFn::LZ4),
            Type::F64(_) => (Types::F64, CompressFn::Decimal(3)),
            Type::Bytes(_) => (Types::Bytes, CompressFn::NOOP),
            _ => unimplemented!(),
        };
        types.push(t);
        compression.push(c);
    });
    let compression = Compression::from(compression);
    let nvars = types.len();
    let conf = SeriesConfig {
        id,
        types,
        compression,
        seg_count: 3,
        nvars,
    };
    conf
}

fn get_span_samples(resource_spans: Vec<ResourceSpans>) -> Vec<(SeriesId, u64, Vec<Type>)> {
    let mut spans = Vec::new();
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
                span.name.hash(&mut hasher);
                let series_id = SeriesId(hasher.finish());

                //let mut v = Vec::with_capacity(1);
                //v.push(Type::Bytes(bincode::serialize(&span).unwrap()));

                let mut v = Vec::with_capacity(5);
                v.push(Type::Bytes(span.trace_id));
                v.push(Type::Bytes(span.span_id));
                v.push(Type::Bytes(span.parent_span_id));
                v.push(Type::Bytes(bincode::serialize(&span.attributes).unwrap()));
                v.push(Type::U64(span.end_time_unix_nano));
                spans.push((series_id, span.start_time_unix_nano, v));
            }
        }
    }
    spans
}

fn writer_worker(args: Args, mut chan: mpsc::Receiver<Vec<(SeriesId, u64, Vec<Type>)>>) {
    let queue_config = KafkaConfig {
        bootstrap: args.kafka_bootstraps.clone(),
        topic: args.kafka_topic.clone(),
    }
    .config();
    let writer_config = WriterConfig {
        queue_config,
        active_block_flush_sz: args.mach_active_block_sz,
    };
    let mut writers = Vec::new();
    writers.push((*MACH).write().unwrap().add_writer(writer_config.clone()).unwrap());
    //writers.push((*MACH).write().unwrap().add_writer(writer_config).unwrap());

    //let mut writer = (*MACH).write().unwrap().add_writer(writer_config).unwrap();
    let mut reference_map: HashMap<SeriesId, (WriterId, SeriesRef)> = HashMap::new();

    while let Ok(mut v) = chan.recv() {
        let _ = *COUNTER_INIT;
        QUEUE_LENGTH.fetch_sub(v.len(), SeqCst);
        let item_count = v.len();
        for (id, ts, values) in v.drain(..) {
            // Get the series ref. If it's not available register the series
            let (wid, id_ref) = *reference_map.entry(id).or_insert_with(|| {
                let conf = get_series_config(id, values.as_slice());
                let (wid, _) = (*MACH).write().unwrap().add_series(conf).unwrap();
                let id_ref = writers[wid.0].get_reference(id);
                (wid, id_ref)
            });
            //println!("wid: {:?}", wid);

            //for _ in 0..2{
            loop {
                if writers[wid.0].push(id_ref, ts, values.as_slice()).is_ok() {
                    break;
                }
            }
        }
        SAMPLES.fetch_add(item_count, SeqCst);
    }
}

fn get_metrics_samples(resource_metrics: Vec<ResourceMetrics>) -> Vec<(SeriesId, u64, Vec<Type>)> {
    let mut items = Vec::new();
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
                        unimplemented!();
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

                            // Push sample
                            let timestamp = point.time_unix_nano;
                            let value = match point.value.as_ref().unwrap() {
                                number_data_point::Value::AsDouble(x) => *x,
                                number_data_point::Value::AsInt(x) => {
                                    let x: i32 = (*x).try_into().unwrap();
                                    x.into()
                                }
                            };
                            let value = vec![Type::F64(value)];
                            items.push((id, timestamp, value));
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

                            // Push sample
                            let timestamp = point.time_unix_nano;
                            let value: Vec<Type> = point.bucket_counts.iter().map(|x| {
                                let x: i32 = (*x).try_into().unwrap();
                                Type::F64(x.into())
                            }).collect();
                            items.push((id, timestamp, value));
                        }
                    },

                    Data::ExponentialHistogram(x) => {
                        unimplemented!();
                    },

                    Data::Summary(x) => {
                        unimplemented!();
                    },
                } // match brace
            } // metric loop
        } // scope loop
    } // resource loop
    items
}

fn batcher(
    mut rx: UnboundedReceiver<Vec<(SeriesId, u64, Vec<Type>)>>,
    tx: mpsc::SyncSender<Vec<(SeriesId, u64, Vec<Type>)>>,
    batchsz: usize,
) {
    //let mut v = Vec::with_capacity(batchsz);
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    while let Some(mut item) = rt.block_on(rx.recv()) {
        tx.send(item);
        //v.append(&mut item);
        //if v.len() > batchsz {
        //    tx.send(v);
        //    v = Vec::with_capacity(batchsz);
        //}
    }
}

fn init_mach(args: Args) -> MachServer {
    let (writer_tx, writer_rx) = mpsc::sync_channel(10);
    let (batcher_tx, batcher_rx) = unbounded_channel();

    std::thread::spawn(move || batcher(batcher_rx, writer_tx, 1));
    std::thread::spawn(move || writer_worker(args, writer_rx));

    let server = MachServer {
        writer: batcher_tx
    };

    server
}

#[tokio::main]
async fn main()  -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    println!("Args: {:#?}", args);
    let mut addr = String::from(args.server_addr.as_str());
    addr.push(':');
    addr.push_str(args.server_port.as_str());
    let addr = addr.parse().unwrap();

    match args.tsdb.as_str() {
        "mach" => {
            let server = init_mach(args);
            Server::builder()
                .add_service(TraceServiceServer::new( server.clone() ))
                //.add_service(LogsServiceServer::new( server.clone() ))
                //.add_service(MetricsServiceServer::new( server.clone() ))
                .serve(addr)
                .await?;
        },
        "kafka" => {
            let (tx, rx) = unbounded_channel();
            let args_clone = args.clone();
            std::thread::spawn(move || { kafka_worker(args_clone, rx) });
            let server = KafkaServer { sender: tx };
            Server::builder()
                .add_service(TraceServiceServer::new( server.clone() ))
                //.add_service(LogsServiceServer::new( server.clone() ))
                //.add_service(MetricsServiceServer::new( server.clone() ))
                .serve(addr)
                .await?;
        }
        _ => unreachable!(), // validation covers this
    }
    Ok(())
}
