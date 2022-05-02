use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

use rpc::tsdb_service_server::TsdbService;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};

use crate::increment_sample_counter;
use crate::rpc;
use crate::tag_index::TagIndex;
use dashmap::DashMap;
use mach::durable_queue::KafkaConfig;
use mach::{
    compression::{CompressFn, Compression},
    durable_queue::QueueConfig,
    id::{SeriesId, WriterId, SeriesRef},
    reader::{ReadResponse, ReadServer},
    sample::Type,
    series::{SeriesConfig, Types},
    tags::Tags,
    tsdb::Mach,
    utils::{bytes::Bytes, random_id},
    writer::{Writer, WriterConfig},
};
use regex::Regex;
use std::{
    collections::HashMap, convert::From, sync::{Arc, mpsc::channel},
};
use futures::executor::block_on;

impl From<QueueConfig> for rpc::QueueConfig {
    fn from(config: QueueConfig) -> Self {
        rpc::QueueConfig {
            configs: match config {
                QueueConfig::Kafka(cfg) => {
                    Some(rpc::queue_config::Configs::Kafka(rpc::KafkaConfig {
                        bootstrap: cfg.bootstrap,
                        topic: cfg.topic,
                    }))
                }
                QueueConfig::File(cfg) => Some(rpc::queue_config::Configs::File(rpc::FileConfig {
                    dir: cfg.dir.into_os_string().into_string().unwrap(),
                    file: cfg.file,
                })),
            },
        }
    }
}

struct WriterWorkerItem {
    series_id: SeriesId,
    samples: Vec<rpc::Sample>,
    response: oneshot::Sender<Vec<rpc::SampleResult>>,
}

fn writer_worker(
    mut writer: Writer,
    mut requests: mpsc::UnboundedReceiver<WriterWorkerItem>,
) {
    let mut references = HashMap::new();
    //let counter = COUNTER.clone();
    println!("Writer id {:?} starting up", writer.id());
    let mut values = Vec::new();

    // Loop over the items being written
    while let Some(mut item) = block_on(requests.recv()) {
        let series_ref = *references
            .entry(item.series_id)
            .or_insert_with(|| writer.get_reference(item.series_id));
        let mut results = Vec::new();

        // Loop over the samples in the item
        for mut sample in item.samples.drain(..) {
            // Iterate over sample's values and create values Mach can interpret
            values.clear();
            for v in sample.values.drain(..) {
                let item = match v.value_type {
                    Some(rpc::value::ValueType::F64(x)) => Type::F64(x),
                    Some(rpc::value::ValueType::Str(x)) => {
                        Type::Bytes(Bytes::from_slice(x.as_bytes()))
                    }
                    _ => panic!("Unhandled value type in sample"),
                };
                values.push(item);
            }

            // Push the sample
            loop {
                if writer
                    .push(series_ref, sample.timestamp, values.as_slice())
                    .is_ok() {
                    increment_sample_counter(1);
                    break;
                }
            }

            // Record result
            results.push(rpc::SampleResult {
                id: sample.id,
                result: true,
            })
        }
        item.response.send(results).unwrap();
    }
}

#[derive(Clone)]
pub struct MachTSDB {
    tag_index: TagIndex,
    tsdb: Arc<RwLock<Mach>>,
    //sources: Arc<DashMap<SeriesId, mpsc::UnboundedSender<WriterWorkerItem>>>,
    //writers: Arc<DashMap<WriterId, mpsc::UnboundedSender<WriterWorkerItem>>>,
    writer: Arc<RwLock<Writer>>,
    references: Arc<DashMap<SeriesId, SeriesRef>>,
    reader: ReadServer,
}

impl MachTSDB {
    pub fn new() -> Self {
        let tag_index = TagIndex::new();
        let mut mach = Mach::new();
        //let writers = DashMap::new();
            let queue_config = KafkaConfig {
                bootstrap: String::from("b-2.demo-cluster-1.c931w3.c25.kafka.us-east-1.amazonaws.com:9092,b-1.demo-cluster-1.c931w3.c25.kafka.us-east-1.amazonaws.com:9092,b-3.demo-cluster-1.c931w3.c25.kafka.us-east-1.amazonaws.com:9092"),
                //bootstrap: String::from("localhost:9093,localhost:9094,localhost:9095"),
                topic: random_id(),
            }
            .config();

            let writer_config = WriterConfig {
                queue_config,
                active_block_flush_sz: 1_000_000,
            };

            let writer = mach.add_writer(writer_config).unwrap();
            //let (tx, rx) = mpsc::unbounded_channel();
            //writers.insert(writer.id(), tx);
            //std::thread::spawn(move || {
            //    writer_worker(writer, rx)
            //});

        let reader = mach.new_read_server();

        Self {
            tag_index,
            tsdb: Arc::new(RwLock::new(mach)),
            references: Arc::new(DashMap::new()),
            writer: Arc::new(RwLock::new(writer)),
            reader,
        }
    }

    pub async fn read_handler(&self, re: &Regex) -> HashMap<Tags, ReadResponse> {
        let tags = self.tag_index.search(re);
        let mut results = HashMap::new();
        for tag in tags {
            let response = self.reader.read_request(tag.id()).await;
            results.insert(tag, response);
        }
        results
    }

    async fn push_stream_handler(
        &self,
        response_channel: mpsc::Sender<Result<rpc::PushResponse, Status>>,
        mut request_stream: tonic::Streaming<rpc::PushRequest>,
        ) {
        let mut writer = self.writer.write().await;
        let mut mach = self.tsdb.write().await;
        let mut values = Vec::new();

        while let Some(Ok(mut request)) = request_stream.next().await {
            let mut responses = Vec::new();
            for mut samples in request.samples.drain(..) {
                let tags = Tags::from(samples.tags.clone());
                let series_id = tags.id();

                // Try to get series reference, otehrwise, register
                let series_ref = *self.references.entry(series_id).or_insert_with(|| {
                    let (w, s) = mach.add_series(detect_config(&tags, &samples.samples[0])).unwrap();
                    //assert_eq!(w, WriterId(0));
                    let r = writer.get_reference(series_id);
                    r
                });

                for mut sample in samples.samples.drain(..) {
                    // Iterate over sample's values and create values Mach can interpret
                    values.clear();
                    for v in sample.values.drain(..) {
                        let item = match v.value_type {
                            Some(rpc::value::ValueType::F64(x)) => Type::F64(x),
                            Some(rpc::value::ValueType::Str(x)) => {
                                Type::Bytes(Bytes::from_slice(x.as_bytes()))
                            }
                            _ => panic!("Unhandled value type in sample"),
                        };
                        values.push(item);
                    }

                    // Push the sample
                    loop {
                        if writer
                            .push(series_ref, sample.timestamp, values.as_slice())
                                .is_ok() {
                                    increment_sample_counter(1);
                                    break;
                                }
                    }

                    // Record result
                    responses.push(rpc::SampleResult {
                        id: sample.id,
                        result: true,
                    })
                }
            }
            //println!("DONE");
            response_channel
                .send(Ok(rpc::PushResponse { responses }))
                .await
                .unwrap();
        }
    }
}

fn detect_config(tags: &Tags, sample: &rpc::Sample) -> SeriesConfig {
    let mut types = Vec::new();
    let mut compression = Vec::new();
    for v in sample.values.iter() {
        match v.value_type {
            Some(rpc::value::ValueType::F64(_)) => {
                types.push(Types::F64);
                compression.push(CompressFn::Decimal(3));
            }
            Some(rpc::value::ValueType::Str(_)) => {
                types.push(Types::Bytes);
                compression.push(CompressFn::BytesLZ4);
            }
            _ => panic!("Unhandled value type in sample"),
        }
    }
    let compression = Compression::from(compression);
    let seg_count = 1;
    let nvars = types.len();

    SeriesConfig {
        tags: tags.clone(),
        types,
        compression,
        seg_count,
        nvars,
    }
}

#[tonic::async_trait]
impl TsdbService for MachTSDB {
    async fn echo(
        &self,
        msg: Request<rpc::EchoRequest>,
    ) -> Result<Response<rpc::EchoResponse>, Status> {
        //println!("Got a request: {:?}", msg);
        let reply = rpc::EchoResponse {
            msg: format!("Echo: {}", msg.into_inner().msg),
        };
        Ok(Response::new(reply))
    }

    type EchoStreamStream = ReceiverStream<Result<rpc::EchoResponse, Status>>;
    async fn echo_stream(
        &self,
        request: Request<tonic::Streaming<rpc::EchoRequest>>,
    ) -> Result<Response<Self::EchoStreamStream>, Status> {
        let mut in_stream = request.into_inner();
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            while let Some(result) = in_stream.next().await {
                match result {
                    Ok(v) => tx
                        .send(Ok(rpc::EchoResponse { msg: v.msg }))
                        .await
                        .expect("working rx"),
                    Err(err) => {
                        eprintln!("Error {:?}", err);
                        match tx.send(Err(err)).await {
                            Ok(_) => (),
                            Err(_err) => break, // response was droped
                        }
                    }
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn read(
        &self,
        msg: Request<rpc::ReadRequest>,
    ) -> Result<Response<rpc::ReadResponse>, Status> {
        let re = Regex::new(msg.into_inner().regex.as_str()).unwrap();
        let mut results = self.read_handler(&re).await;

        let mut snapshots = Vec::new();
        for (tags, r) in results.drain() {
            let response_queue = rpc::QueueConfig::from(r.response_queue);
            let data_queue = rpc::QueueConfig::from(r.data_queue);
            snapshots.push(rpc::SeriesSnapshot {
                tags: tags.into(),
                response_queue: Some(response_queue),
                data_queue: Some(data_queue),
                offset: r.offset,
            });
        }

        Ok(Response::new(rpc::ReadResponse { snapshots }))
    }

    type PushStreamStream = ReceiverStream<Result<rpc::PushResponse, Status>>;
    async fn push_stream(
        &self,
        request: Request<tonic::Streaming<rpc::PushRequest>>,
    ) -> Result<Response<Self::PushStreamStream>, Status> {
        let stream = request.into_inner();
        let (tx, rx) = mpsc::channel(1);

        let this = self.clone();
        tokio::task::spawn(async move { this.push_stream_handler(tx, stream).await });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
