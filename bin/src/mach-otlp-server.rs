
use tonic::{transport::Server, Request, Response, Status};
use tokio_stream::wrappers::UnboundedReceiverStream;
use prost::Message;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender, UnboundedReceiver};
use std::sync::{Arc, Mutex};
use std::hash::{Hasher};
use std::fs::File;
use std::io::prelude::*;
use clap::Parser;

//use mach::{
//    durable_queue::{KafkaConfig},
//    series::{Types, SeriesConfig},
//    compression::{CompressFn, Compression},
//    id::{WriterId, SeriesId, SeriesRef},
//    utils::random_id,
//    tsdb::Mach,
//    writer::WriterConfig,
//    sample::Type,
//};

use mach_otlp::collector::{
    trace::v1::{
        trace_service_server::{TraceService, TraceServiceServer},
        ExportTraceServiceRequest, ExportTraceServiceResponse,
    },
};


#[derive(Parser, Debug, Clone)]
struct Args {

    #[clap(short, long, default_value_t = String::from("0.0.0.0"))]
    server_addr: String,

    #[clap(short, long, default_value_t = String::from("50050"))]
    server_port: String,

    #[clap(short, long, default_value_t = String::from("example"), parse(try_from_str=validate_tsdb))]
    tsdb: String,

    //#[clap(short, long, default_value_t = String::from("localhost:9093,localhost:9094,localhost:9095"))]
    //kafka_bootstraps: String,

    //#[clap(short, long, default_value_t = random_id())]
    //kafka_topic: String,

    //#[clap(short, long, default_value_t = 1)]
    //kafka_partitions: i32,

    //#[clap(short, long, default_value_t = 3)]
    //kafka_replication: i32,

    //#[clap(short, long, default_value_t = String::from("all"), parse(try_from_str=validate_ack))]
    //kafka_acks: String,

    //#[clap(short, long, default_value_t = 8192)]
    //kafka_batch: usize,

    #[clap(short, long)]
    file_path: Option<String>,

    #[clap(short, long, default_value_t = 1000000)]
    file_item_count: usize,

    //#[clap(short, long, default_value_t = 1000000)]
    //mach_active_block_sz: usize,
}

fn validate_tsdb(s: &str) -> Result<String, String> {
    Ok(match s {
        //"mach" => s.into(),
        //"kafka" => s.into(),
        "example" => s.into(),
        "file" => s.into(),
        _ => return Err(format!("Invalid option {}, valid options are \"example\", \"file\".", s))
    })
}

//
//fn validate_ack(s: &str) -> Result<String, String> {
//    Ok(match s {
//        "all" => s.into(),
//        "1" => s.into(),
//        "0" => s.into(),
//        "2" => s.into(),
//        _ => return Err("Valid options are: all, \"1\", \"2\", \"3\"".into())
//    })
//}

trait OtlpProcessor: Sync + Send + Clone {
    fn process(&self, data: mach_otlp::OtlpData);
}

#[derive(Clone)]
struct ExampleOtlpProcessor {}

impl OtlpProcessor for ExampleOtlpProcessor {
    fn process(&self, _data: mach_otlp::OtlpData) {
        //let sample_count = data.sample_count();
        //println!("Got {} spans", sample_count);
    }
}

#[derive(Clone)]
struct FileOtlpProcessor {
    vec: Arc<Mutex<Vec<mach_otlp::OtlpData>>>,
    filename: String,
    len: usize,
}

impl FileOtlpProcessor {
    fn new(filename: String, len: usize) -> Self {
        Self {
            vec: Arc::new(Mutex::new(Vec::new())),
            filename,
            len,
        }
    }
}

impl OtlpProcessor for FileOtlpProcessor {
    fn process(&self, data: mach_otlp::OtlpData) {
        let mut guard = self.vec.lock().unwrap();
        guard.push(data);
        println!("{}", guard.len());
        if guard.len() > self.len {
            let bytes = bincode::serialize(&*guard).unwrap();
            let mut file = File::create(self.filename.as_str()).unwrap();
            file.write_all(bytes.as_slice()).unwrap();
            println!("Wrote file, can quit now");
            guard.clear();
            println!("Cleared vector, restarting");
        }
    }
}

#[derive(Clone)]
struct OtlpServer<P: OtlpProcessor> {
    processor: P
}

#[tonic::async_trait]
impl<P: OtlpProcessor + 'static> TraceService for OtlpServer<P> {
    async fn export(
        &self,
        req: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let data = mach_otlp::OtlpData::Spans(req.into_inner().resource_spans);
        self.processor.process(data);
        Ok(Response::new(ExportTraceServiceResponse {}))
    }
}

//#[tonic::async_trait]
//impl<P: OtlpProcessor + 'static> LogsService for OtlpServer<P> {
//    async fn export(
//        &self,
//        req: Request<ExportLogsServiceRequest>,
//    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
//        unimplemented!()
//    }
//}
//
//#[tonic::async_trait]
//impl<P: OtlpProcessor + 'static> MetricsService for OtlpServer<P> {
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
        "example" => {
            let server = OtlpServer{
                processor: ExampleOtlpProcessor{},
            };
            let trace_server = TraceServiceServer::new(server);
            Server::builder()
                .add_service(trace_server)
                .serve(addr)
                .await?;
            },
        _ => unimplemented!(),
    };
    Ok(())
}

//fn init_counters() {
//    let mut samples = [0; 5];
//    let mut items = [0; 5];
//    std::thread::spawn(move || {
//        for i in 0.. {
//            std::thread::sleep(Duration::from_secs(1));
//            let idx = i % 5;
//
//            samples[idx] = SAMPLES.load(SeqCst);
//            let max = samples.iter().max().unwrap();
//            let min = samples.iter().min().unwrap();
//            let sample_rate = (max - min) / 5;
//
//            items[idx] =ITEMS.load(SeqCst);
//            let max = items.iter().max().unwrap();
//            let min = items.iter().min().unwrap();
//            let items_rate = (max - min) / 5;
//
//            let ql = QUEUE_LENGTH.load(SeqCst);
//            println!("{} samples / sec, {} items / sec, queue length {}", sample_rate, items_rate, ql);
//        }
//    });
//}
//
//lazy_static! {
//    pub static ref QUEUE_LENGTH: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
//    pub static ref ITEMS: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
//    pub static ref SAMPLES: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
//    pub static ref COUNTER_INIT: bool = {
//        init_counters();
//        true
//    };
//    pub static ref MACH: Arc<RwLock<Mach>> = Arc::new(RwLock::new(Mach::new()));
//    pub static ref TAG_INDEX: TagIndex = TagIndex::new();
//}


