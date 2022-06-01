mod otlp;
mod tag_index;
mod id_index;
mod mach_proto;

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
            trace_stream_service_client::{TraceStreamServiceClient},
            ExportTraceServiceRequest, ExportTraceServiceResponse,
        },
    },
    OtlpData,
};

use tonic::{transport::Server, Request, Response, Status};
use tokio_stream::wrappers::UnboundedReceiverStream;
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
        let data = OtlpData::Metrics(req.into_inner().resource_metrics);
        let sample_count = data.sample_count();
        let bytes = bincode::serialize(&data).unwrap();

        if self.sender.send((sample_count, bytes)).is_err() {
            panic!("Failed to write to sender");
        } else {
            QUEUE_LENGTH.fetch_add(1, SeqCst);
        }
        Ok(Response::new(ExportMetricsServiceResponse {}))
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
    stream: UnboundedSender<mach_proto::ExportMessage>
}

impl MachServer {
    async fn new(args: Args) -> Self {
        let mut client = mach_proto::MachServiceClient::connect("http://0.0.0.0:4318").await.unwrap();
        let (tx, rx) = unbounded_channel();
        let rx = UnboundedReceiverStream::new(rx);
        tokio::task::spawn(async move {
            let _ = client.export(rx).await.unwrap().into_inner();
        });
        MachServer {
            stream: tx
        }
    }
}

#[tonic::async_trait]
impl TraceService for MachServer {
    async fn export(
        &self,
        req: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let resource_spans = req.into_inner().resource_spans;
        let mut samples = Vec::new();
        resource_spans.iter().for_each(|x| samples.append(&mut x.get_samples()));
        let msg = mach_proto::ExportMessage {
            data: bincode::serialize(&samples).unwrap(),
        };
        self.stream.send(msg).unwrap();
        Ok(Response::new(ExportTraceServiceResponse {}))
    }
}

#[tonic::async_trait]
impl MetricsService for MachServer {
    async fn export(
        &self,
        req: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let resource_metrics = req.into_inner().resource_metrics;
        let mut samples = Vec::new();
        resource_metrics.iter().for_each(|x| samples.append(&mut x.get_samples()));
        let msg = mach_proto::ExportMessage {
            data: bincode::serialize(&samples).unwrap(),
        };
        self.stream.send(msg).unwrap();
        Ok(Response::new(ExportMetricsServiceResponse {}))
    }
}

//#[tonic::async_trait]
//impl LogsService for MachServer {
//    async fn export(
//        &self,
//        req: Request<ExportLogsServiceRequest>,
//    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
//        unimplemented!()
//    }
//}
//
//#[tonic::async_trait]
//impl MetricsService for MachServer {
//    async fn export(
//        &self,
//        req: Request<ExportMetricsServiceRequest>,
//    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
//        unimplemented!()
//    }
//}

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
            let server = MachServer::new(args).await;
            Server::builder()
                .add_service(TraceServiceServer::new( server.clone() ))
                .add_service(MetricsServiceServer::new( server.clone() ))
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
                .add_service(MetricsServiceServer::new( server.clone() ))
                .serve(addr)
                .await?;
        }
        _ => unreachable!(), // validation covers this
    }
    Ok(())
}
