use tokio::sync::RwLock;
use tonic::{transport::Server, Request, Response, Status};

pub mod mach_rpc {
    tonic::include_proto!("mach_rpc"); // The string specified here must match the proto package name
}

use mach_rpc::tsdb_service_server::{TsdbService, TsdbServiceServer};
use mach_rpc as rpc;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};

//mod reader;
mod tag_index;
//mod writer;
#[allow(unused_imports)]
use mach::durable_queue::{FileConfig, KafkaConfig};
use mach::{
    compression::{CompressFn, Compression},
    series::{SeriesConfig, Types},
    sample::Type,
    tags::Tags,
    tsdb::Mach,
    utils::{random_id, bytes::Bytes},
    writer::{Writer, WriterConfig},
    id::{SeriesId, WriterId},
    reader::{ReadServer, ReadResponse},
    durable_queue::QueueConfig,
};
use tag_index::TagIndex;
use std::{time::Duration, convert::From, collections::{HashMap}, sync::{Arc, atomic::{AtomicUsize, Ordering::SeqCst}}};
use dashmap::DashMap;
use lazy_static::lazy_static;
use regex::Regex;

lazy_static! {
    static ref COUNTER: Arc<AtomicUsize> = {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();
        tokio::spawn(async move {
            let mut last = 0;
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let cur = counter_clone.load(SeqCst);
                println!("received {} / sec", cur - last);
                last = cur;
            }
        });
        counter
    };
}

impl From<QueueConfig> for rpc::QueueConfig {
    fn from(config: QueueConfig) -> Self {
        rpc::QueueConfig {
            configs: match config {
                QueueConfig::Kafka(cfg) => {
                    Some(rpc::queue_config::Configs::Kafka(mach_rpc::KafkaConfig {
                        bootstrap: cfg.bootstrap,
                        topic: cfg.topic,
                    }))
                }
                QueueConfig::File(cfg) => Some(rpc::queue_config::Configs::File(mach_rpc::FileConfig {
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

async fn writer_worker(
    mut writer: Writer,
    mut requests: mpsc::UnboundedReceiver<WriterWorkerItem>) {
    let mut references = HashMap::new();
    let counter = COUNTER.clone();
    println!("Writer id {:?} starting up", writer.id());
    let mut values = Vec::new();

    // Loop over the items being written
    while let Some(mut item) = requests.recv().await {
        let series_ref = *references.entry(item.series_id).or_insert_with(|| {
            writer.get_reference(item.series_id)
        });
        let mut results = Vec::new();

        // Loop over the samples in the item
        for mut sample in item.samples.drain(..) {

            // Iterate over sample's values and create values Mach can interpret
            values.clear();
            for v in sample.values.drain(..) {
                let item = match v.value_type {
                    Some(rpc::value::ValueType::F64(x)) => Type::F64(x),
                    Some(rpc::value::ValueType::Str(x)) => Type::Bytes(Bytes::from_slice(x.as_bytes())),
                    _ => panic!("Unhandled value type in sample"),
                    };
                values.push(item);
            }

            // Push the sample
            let result = writer.push(series_ref, sample.timestamp, values.as_slice()).is_ok();

            // Record result
            results.push(rpc::SampleResult {
                id: sample.id,
                result,
            })
        }
        counter.fetch_add(results.len(), SeqCst);
        item.response.send(results).unwrap();
    }
}

#[derive(Clone)]
pub struct MachTSDB {
    tag_index: TagIndex,
    tsdb: Arc<RwLock<Mach>>,
    sources: Arc<DashMap<SeriesId, mpsc::UnboundedSender<WriterWorkerItem>>>,
    writers: Arc<DashMap<WriterId, mpsc::UnboundedSender<WriterWorkerItem>>>,
    reader: ReadServer,
}

impl MachTSDB {
    fn new() -> Self {
        let tag_index = TagIndex::new();
        let mut mach = Mach::new();
        let writers = DashMap::new();
        for _ in 0..1 {
            let queue_config = KafkaConfig {
                bootstrap: String::from("localhost:9093,localhost:9094,localhost:9095"),
                topic: random_id(),
            }
            .config();

            let writer_config = WriterConfig {
                queue_config,
                active_block_flush_sz: 1_000_000,
            };

            let writer = mach.add_writer(writer_config).unwrap();
            let (tx, rx) = mpsc::unbounded_channel();
            writers.insert(writer.id(), tx);
            tokio::task::spawn(writer_worker(writer, rx));
        }

        let reader = mach.new_read_server();

        Self {
            tag_index,
            tsdb: Arc::new(RwLock::new(mach)),
            sources: Arc::new(DashMap::new()),
            writers: Arc::new(writers),
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
        mut request_stream: tonic::Streaming<rpc::PushRequest>
    ) {
        while let Some(Ok(mut request)) = request_stream.next().await {
            let mut responses = Vec::new();
            for samples in request.samples.drain(..) {
                let tags = Tags::from(samples.tags);
                let series_id = tags.id();
                let (response_sender, response_receiver) = oneshot::channel();

                // Already registered
                if let Some(sender) = self.sources.get(&tags.id()) {
                    //println!("NO REGISTER");
                    let item = WriterWorkerItem {
                        series_id,
                        samples: samples.samples,
                        response: response_sender
                    };
                    if sender.send(item).is_err() {
                        panic!("Send to writer worker error");
                    }
                    let response = response_receiver.await.unwrap();
                    responses.extend_from_slice(&response);
                }

                // Register first
                else {
                    let config = detect_config(tags.clone(), &samples.samples[0]);
                    let (writer_id, series_id) = self.tsdb.write().await.add_series(config).unwrap();
                    let sender = self.writers.get(&writer_id).unwrap().clone();
                    self.sources.insert(series_id, sender.clone());
                    let item = WriterWorkerItem {
                        series_id,
                        samples: samples.samples,
                        response: response_sender
                    };
                    if sender.send(item).is_err() {
                        panic!("Send to writer worker error");
                    }
                    let response = response_receiver.await.unwrap();
                    self.tag_index.insert(tags);
                    responses.extend_from_slice(&response);
                }
            }
            //println!("DONE");
            response_channel.send(Ok(rpc::PushResponse { responses })).await.unwrap();
        }
    }
}

fn detect_config(tags: Tags, sample: &rpc::Sample) -> SeriesConfig {
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
        tags,
        types,
        compression,
        seg_count,
        nvars
    }
}

#[tonic::async_trait]
impl TsdbService for MachTSDB {
    async fn echo(&self, msg: Request<rpc::EchoRequest>) -> Result<Response<rpc::EchoResponse>, Status> {
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

    async fn read(&self, msg: Request<rpc::ReadRequest>) -> Result<Response::<rpc::ReadResponse>, Status> {
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

        Ok(Response::new(rpc::ReadResponse { snapshots } ))
    }

    type PushStreamStream = ReceiverStream<Result<rpc::PushResponse, Status>>;
    async fn push_stream(
        &self,
        request: Request<tonic::Streaming<rpc::PushRequest>>
    ) -> Result<Response<Self::PushStreamStream>, Status> {
        let stream = request.into_inner();
        let (tx, rx) = mpsc::channel(1);

        let this = self.clone();
        tokio::task::spawn(async move { this.push_stream_handler(tx, stream).await });


        Ok(Response::new(ReceiverStream::new(rx)))
    }

}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:50050".parse()?;
    let tsdb = MachTSDB::new();

    Server::builder()
        .add_service(TsdbServiceServer::new(tsdb))
        .serve(addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
