#![allow(warnings)]
//mod otlp;
//mod mach_proto;
//mod tag_index;
//mod id_index;


//use otlp::{
//    metrics::v1::{metric::Data, MetricsData, number_data_point, ResourceMetrics},
//    logs::v1::{LogsData},
//    trace::v1::{TracesData, ResourceSpans, Span},
//    collector::{
//        logs::v1::{
//            logs_service_server::{LogsService, LogsServiceServer},
//            ExportLogsServiceRequest, ExportLogsServiceResponse,
//        },
//        metrics::v1::{
//            metrics_service_server::{MetricsService, MetricsServiceServer},
//            ExportMetricsServiceRequest, ExportMetricsServiceResponse,
//        },
//        trace::v1::{
//            //trace_service_server::{TraceService, TraceServiceServer},
//            ExportTraceServiceRequest, ExportTraceServiceResponse,
//        },
//    },
//    OtlpData,
//};
//
//use tonic::{transport::Server, Request, Response, Status};
//use tokio::sync::mpsc::{unbounded_channel, UnboundedSender, UnboundedReceiver};
//use tokio_stream::{wrappers::ReceiverStream, StreamExt};
//use std::sync::{Arc, RwLock, atomic::{AtomicUsize, Ordering::SeqCst}};
//use std::sync::mpsc;
use std::time::Duration;
use std::collections::{HashMap, HashSet, hash_map::DefaultHasher};
//use std::hash::{Hash, Hasher};
use std::fs::File;
use std::io::prelude::*;
//use lazy_static::*;
use clap::Parser;

use mach::{
    durable_queue::{NoopConfig, KafkaConfig, TOTAL_SZ},
    series::{Types, SeriesConfig},
    compression::{CompressFn, Compression},
    id::{WriterId, SeriesId, SeriesRef},
    utils::random_id,
    tsdb::Mach,
    writer::{Writer, WriterConfig},
    sample::Type,
};

fn get_series_config(id: SeriesId, values: &[Type]) -> SeriesConfig {
    let mut types = Vec::new();
    let mut compression = Vec::new();
    values.iter().for_each(|v| {
        let (t, c) = match v {
            Type::U32(_) => (Types::U32, CompressFn::IntBitpack),
            Type::U64(_) => (Types::U64, CompressFn::LZ4),
            Type::F64(_) => (Types::F64, CompressFn::Decimal(3)),
            Type::Bytes(_) => (Types::Bytes, CompressFn::NOOP),
            Type::BorrowedBytes(_) => (Types::Bytes, CompressFn::NOOP),
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

fn kafka_ingest(args: Args, mut data: Vec<mach_otlp::OtlpData>) {
    use rdkafka::{
        admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
        client::DefaultClientContext,
        config::ClientConfig,
        producer::{FutureProducer, FutureRecord},
    };
    use zstd::stream::{encode_all};
    use prost::Message;

    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let topic = random_id();

    let client: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &args.kafka_bootstraps)
        .create()
        .unwrap();
    let admin_opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(3)));
    let topics = &[NewTopic {
        name: topic.as_str(),
        num_partitions: 1,
        replication: TopicReplication::Fixed(3),
        config: vec![("min.insync.replicas", "3")],
    }];
    rt.block_on(client.create_topics(topics, &admin_opts)).unwrap();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", args.kafka_bootstraps)
        .set("acks", "all")
        .set("linger.ms", "0")
        .set("compression.type", "none")
        .set("message.max.bytes", "1000000000")
        .set("message.copy.max.bytes", "1000000000")
        .create()
        .unwrap();

    //let mut byte_buffer: Vec<u8> = Vec::with_capacity(2usize.pow(15));
    let mut buf = Vec::new();
    let now = std::time::Instant::now();
    let written = data.len() as u32;
    let mut sz = 0;
    let mut uncompressed = 0u32;
    for item in data {
        let serialized_sz = bincode::serialized_size(&item).unwrap() as usize;
        let len = buf.len();
        buf.resize(buf.len() + serialized_sz, 0);
        bincode::serialize_into(&mut buf[len..], &item).unwrap();
        //match item {
        //    mach_otlp::OtlpData::Spans(x) => {
        //        let x = mach_otlp::trace::v1::TracesData { resource_spans: x };
        //        let sz = x.encoded_len();
        //        let buf_len = buf.len();
        //        buf.resize(buf_len + sz, 0);
        //        let mut slice: &mut [u8] = &mut buf[buf_len..];
        //        x.encode(&mut slice);
        //    }
        //    _ => unimplemented!()
        //}
        if buf.len() > 1000000 {
            let sl = buf.as_slice();
            uncompressed += sl.len() as u32;
            //let bytes = encode_all(sl, 0).unwrap();
            //let to_send: FutureRecord<str, [u8]> =
            //    FutureRecord::to(&topic).payload(&bytes);
            //match rt.block_on(producer.send(to_send, Duration::from_secs(3))) {
            //    Ok(x) => {},
            //    Err(_) => panic!("Producer failed"),
            //};
            //sz += bytes.len();
            buf.clear();
        }
    }
    let elapsed = now.elapsed();
    let elapsed_sec = elapsed.as_secs_f64();

    let written: f64 = written.try_into().unwrap();
    let sz: f64 = (sz as u32).try_into().unwrap();
    println!("Bytes Written {}, Elapsed {:?}, items/sec {}, bytes/sec {}", sz, elapsed, written/elapsed_sec, sz / elapsed_sec );
    println!("Uncompressed {}", uncompressed);
}

#[inline(never)]
fn mach_ingest(args: Args, mut data: &[mach_otlp::OtlpData]) {
    let mut mach = Mach::new();
    let mut reference_map: HashMap<SeriesId, SeriesRef> = HashMap::new();
    let mut id_dict = otlp::SpanIds::new();

    //let queue_config = KafkaConfig {
    //    bootstrap: args.kafka_bootstraps.clone(),
    //    topic: random_id(),
    //}.config();

    let queue_config = NoopConfig{}.config();

    let writer_config = WriterConfig {
        queue_config,
        active_block_flush_sz: 1_000_000,
    };
    let mut writer = mach.add_writer(writer_config.clone()).unwrap();

    let written = data.len() as u32;
    let mut spans = Vec::with_capacity(8192);

    // Extract samples
    let now = std::time::Instant::now();
    for item in data.iter() {
        match item {
            mach_otlp::OtlpData::Spans(x) => {
                for item in x {
                    //write_samples_to_mach(item, &mut reference_map, &mut mach, &mut writer);
                    item.get_samples(&mut spans);
                    //spans.append(&mut samples);
                }
            },
            _ => unimplemented!(),
        }
    }
    let extract_time = now.elapsed();

    // Push data
    let now = std::time::Instant::now();
    let mut skipped = 0;
    let mut tries = Vec::with_capacity(spans.len());
    for (id, ts, values) in spans.iter() {
        let id = *id;
        let ts = *ts;
        let id_ref = *reference_map.entry(id).or_insert_with(|| {
            let conf = get_series_config(id, values.as_slice());
            let (wid, _) = mach.add_series(conf).unwrap();
            let id_ref = writer.get_reference(id);
            id_ref
        });
        let mut failed = 1;
        //for _ in 0..10 {
        let mut try_count = 0;
        loop {
            try_count += 1;
            if writer.push(id_ref, ts, values.as_slice()).is_ok() {
                failed = 0;
                break;
            }
        };
        tries.push(try_count);
        skipped += failed;
    }
    let push_time = now.elapsed();
    let elapsed = extract_time + push_time;
    let elapsed_sec = elapsed.as_secs_f64();
    let written: f64 = written.try_into().unwrap();
    println!("Written {}, Elapsed {:?}, Samples/sec {}", written, elapsed, written/elapsed_sec);
    println!("Sample extraction {:?}, Push {:?}", extract_time, push_time);
    let total_sz_written = TOTAL_SZ.load(std::sync::atomic::Ordering::SeqCst);
    println!("Total Size written: {}", total_sz_written);
    println!("Total skipped: {} / {}", skipped, spans.len());
    let total_tries: usize = tries.iter().sum();
    println!("Tries: {} / {}", total_tries, tries.len());
    println!("Number of series: {}", reference_map.len());
}

#[derive(Parser, Debug, Clone)]
struct Args {

    #[clap(short, long, default_value_t = String::from("mach"))]
    tsdb: String,

    #[clap(short, long, default_value_t = String::from("localhost:9093,localhost:9094,localhost:9095"))]
    kafka_bootstraps: String,

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
    file_path: String,

    //#[clap(short, long, default_value_t = 1000000)]
    //mach_active_block_sz: usize,
}

fn main() {
    let args = Args::parse();
    println!("Args: {:#?}", args);

    let mut data = Vec::new();
    println!("Loading data");
    File::open(args.file_path.as_str()).unwrap().read_to_end(&mut data).unwrap();
    let mut data: Vec<otlp::OtlpData> = bincode::deserialize(data.as_slice()).unwrap();

    let mut now = std::time::SystemTime::now();
    for item in data.iter_mut() {
        let ts: u64 = (now.duration_since(std::time::UNIX_EPOCH)).unwrap().as_nanos().try_into().unwrap();
        item.update_timestamp(ts);
        now += std::time::Duration::from_secs(1);
    }

    let mut data: Vec<mach_otlp::OtlpData> = data.iter().map(|x| {
        let mut x: mach_otlp::OtlpData = x.into();
        match &mut x {
            mach_otlp::OtlpData::Spans(x) => x.iter_mut().for_each(|x| x.set_source_id()),
            _ => unimplemented!(),
        }
        x
    }).collect();

    println!("data items: {}", data.len());
    kafka_ingest(args.clone(), data);
    //mach_ingest(args.clone(), data.as_slice());
}



