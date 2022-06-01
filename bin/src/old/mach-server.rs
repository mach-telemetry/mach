mod otlp;
mod mach_proto;
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
            //trace_service_server::{TraceService, TraceServiceServer},
            trace_stream_service_server::{TraceStreamService, TraceStreamServiceServer},
            ExportTraceServiceRequest, ExportTraceServiceResponse,
        },
    },
    OtlpData,
};

use tonic::{transport::Server, Request, Response, Status};
use prost::Message;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender, UnboundedReceiver};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
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
    writer::{Writer, WriterConfig},
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

    #[clap(short, long, default_value_t = String::from("4318"))]
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

fn new_writer(args: Args) -> Writer {
    println!("Write worker");
    let queue_config = KafkaConfig {
        bootstrap: args.kafka_bootstraps.clone(),
        topic: random_id(),
    }
    .config();
    let writer_config = WriterConfig {
        queue_config,
        active_block_flush_sz: args.mach_active_block_sz,
    };
    (*MACH).write().unwrap().add_writer(writer_config.clone()).unwrap()
}

#[derive(Clone)]
pub struct MachServer {
    args: Args,
}

#[tonic::async_trait]
impl mach_proto::MachService for MachServer {
    async fn export(
        &self,
        request: Request<tonic::Streaming<mach_proto::ExportMessage>>,
    ) -> Result<Response<mach_proto::ExportResponse>, Status> {

        println!("Write worker");
        let queue_config = KafkaConfig {
            bootstrap: self.args.kafka_bootstraps.clone(),
            topic: random_id(),
        }
        .config();
        let writer_config = WriterConfig {
            queue_config,
            active_block_flush_sz: self.args.mach_active_block_sz,
        };
        let mut writers = Vec::new();
        writers.push(new_writer(self.args.clone()));

        let mut reference_map: HashMap<SeriesId, (WriterId, SeriesRef)> = HashMap::new();

        let mut stream = request.into_inner();
        while let Some(Ok(mut request)) = stream.next().await {
            let _ = *COUNTER_INIT;
            let mut spans: Vec<(SeriesId, u64, Vec<Type>)> = bincode::deserialize(&request.data).unwrap();
            let item_count = spans.len();
            for (id, ts, values) in spans.drain(..) {
                // Get the series ref. If it's not available register the series
                let (wid, id_ref) = *reference_map.entry(id).or_insert_with(|| {
                    let conf = get_series_config(id, values.as_slice());
                    let (wid, _) = (*MACH).write().unwrap().add_series(conf).unwrap();
                    let id_ref = writers[wid.0].get_reference(id);
                    (wid, id_ref)
                });
                loop {
                    if writers[wid.0].push(id_ref, ts, values.as_slice()).is_ok() {
                        break;
                    }
                }
            }
            SAMPLES.fetch_add(item_count, SeqCst);
        }
        Ok(Response::new(mach_proto::ExportResponse{}))
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

#[tokio::main]
async fn main()  -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    println!("Args: {:#?}", args);
    let mut addr = String::from(args.server_addr.as_str());
    addr.push(':');
    addr.push_str(args.server_port.as_str());
    let addr = addr.parse().unwrap();
    //let server = init_mach(args);
    Server::builder()
        .add_service(mach_proto::MachServiceServer::new( MachServer {args} ))
        //.add_service(LogsServiceServer::new( server.clone() ))
        //.add_service(MetricsServiceServer::new( server.clone() ))
        .serve(addr)
        .await?;

    Ok(())
}
