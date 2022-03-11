use tonic::{transport::Server, Request, Response, Status};

pub mod mach_rpc {
    tonic::include_proto!("mach_rpc"); // The string specified here must match the proto package name
}

use mach::{
    compression::{CompressFn, Compression},
    id::{SeriesId, SeriesRef},
    persistent_list::VectorBackend,
    sample::Type,
    series::{SeriesConfig, Types},
    tags::Tags,
    tsdb::Mach,
    utils::bytes::Bytes,
    writer::Writer,
};
use mach_rpc::writer_service_server::{WriterService, WriterServiceServer};
use mach_rpc::{
    value::PbType, EchoRequest, EchoResponse, GetSeriesReferenceRequest,
    GetSeriesReferenceResponse, MapRequest, MapResponse, PushRequest, PushResponse, Sample, Value,
};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc, Mutex,
    },
};
use std::{error::Error, io::ErrorKind, net::ToSocketAddrs, pin::Pin, time::Duration};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};

enum WriteRequest {
    GetReferenceId(GetReferenceId),
    Push(Push),
    MapTest(MapTest),
}

struct GetReferenceId {
    series_id: u64,
    resp: oneshot::Sender<u64>,
}

struct Push {
    samples: HashMap<u64, Sample>,
    resp: oneshot::Sender<HashMap<u64, bool>>,
}

struct MachSample {
    timestamp: u64,
    values: Vec<Type>,
}

fn pb_sample_to_mach_sample(sample: Sample) -> MachSample {
    let timestamp = sample.timestamp;
    let mut values = Vec::with_capacity(sample.values.len());
    for item in sample.values {
        values.push(pb_type_to_mach_type(item.pb_type.unwrap()));
    }
    MachSample { timestamp, values }
}

fn pb_type_to_mach_type(v: PbType) -> Type {
    match v {
        PbType::F64(x) => Type::F64(x),
        PbType::Str(x) => Type::Bytes(Bytes::from_slice(x.as_bytes())),
    }
}

struct MapTest {
    samples: HashMap<u64, u64>,
    resp: oneshot::Sender<HashMap<u64, bool>>,
}

async fn counter_watcher(counter: Arc<AtomicUsize>) {
    tokio::spawn(async move {
        let mut last = 0;
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let cur = counter.load(SeqCst);
            println!("received {} / sec", cur - last);
            last = cur;
        }
    });
}

async fn writer_worker(mut writer: Writer, mut channel: mpsc::UnboundedReceiver<WriteRequest>) {
    println!("Launching worker");
    let counter = Arc::new(AtomicUsize::new(0));
    counter_watcher(counter.clone()).await;
    while let Some(item) = channel.recv().await {
        match item {
            WriteRequest::GetReferenceId(x) => {
                let reference = writer.get_reference(SeriesId(x.series_id)).0 as u64;
                x.resp.send(reference);
            }
            WriteRequest::MapTest(x) => {
                let response: HashMap<u64, bool> =
                    x.samples.iter().map(|(k, _)| (*k, true)).collect();
                let len = response.len();
                x.resp.send(response);
                counter.fetch_add(len, SeqCst);
            }
            WriteRequest::Push(mut x) => {
                let mut hashmap = HashMap::new();
                for (series_ref, pb_sample) in x.samples.drain() {
                    let sample = pb_sample_to_mach_sample(pb_sample);
                    match writer.push_type(
                        SeriesRef(series_ref as usize),
                        sample.timestamp,
                        &sample.values,
                    ) {
                        Ok(_) => hashmap.insert(series_ref, true),
                        Err(_) => hashmap.insert(series_ref, false),
                    };
                }
                let len = hashmap.len();
                x.resp.send(hashmap);
                counter.fetch_add(len, SeqCst);
            }
        }
    }
}

async fn map_test_worker(
    mut in_stream: tonic::Streaming<MapRequest>,
    client: mpsc::Sender<Result<MapResponse, Status>>,
    writer: mpsc::UnboundedSender<WriteRequest>,
) {
    tokio::spawn(async move {
        while let Some(result) = in_stream.next().await {
            match result {
                Ok(v) => {
                    let (resp_tx, resp_rx) = oneshot::channel();

                    let request = WriteRequest::MapTest(MapTest {
                        samples: v.samples,
                        resp: resp_tx,
                    });
                    if let Err(_) = writer.send(request) {
                        panic!("Writer thread is dead");
                    }
                    let response = resp_rx.await.unwrap();
                    client
                        .send(Ok(MapResponse {
                            samples: response.into(),
                        }))
                        .await
                        .expect("working rx")
                }
                Err(err) => {
                    eprintln!("Error {:?}", err);
                    match client.send(Err(err)).await {
                        Ok(_) => (),
                        Err(_err) => break, // response was droped
                    }
                }
            }
        }
    });
}

async fn push_worker(
    mut in_stream: tonic::Streaming<PushRequest>,
    client: mpsc::Sender<Result<PushResponse, Status>>,
    writer: mpsc::UnboundedSender<WriteRequest>,
) {
    tokio::spawn(async move {
        while let Some(req) = in_stream.next().await {
            match req {
                Ok(v) => {
                    let (resp_tx, resp_rx) = oneshot::channel();

                    let request = WriteRequest::Push(Push {
                        samples: v.samples,
                        resp: resp_tx,
                    });
                    if let Err(_) = writer.send(request) {
                        panic!("Writer thread is dead");
                    }
                    let response = resp_rx.await.unwrap();
                    client
                        .send(Ok(PushResponse {
                            results: response.into(),
                        }))
                        .await
                        .expect("working rx")
                }
                Err(err) => {
                    eprintln!("Error {:?}", err);
                    match client.send(Err(err)).await {
                        Ok(_) => (),
                        Err(_err) => break, // response was droped
                    }
                }
            }
        }
    });
}

pub fn serve_writer(writer: Writer, addr: &str) {
    let w = WriterServiceServer::new(MachWriter::new(writer));
    tokio::spawn(
        Server::builder()
            .add_service(w)
            .serve(addr.parse().unwrap()),
    );
}

pub struct MachWriter {
    sender: mpsc::UnboundedSender<WriteRequest>,
}

impl MachWriter {
    pub fn new(writer: Writer) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(writer_worker(writer, rx));
        Self { sender: tx }
    }
}

#[tonic::async_trait]
impl WriterService for MachWriter {
    async fn echo(&self, msg: Request<EchoRequest>) -> Result<Response<EchoResponse>, Status> {
        //println!("Got a request: {:?}", msg);
        let reply = EchoResponse {
            msg: format!("Echo: {}", msg.into_inner().msg).into(),
        };
        Ok(Response::new(reply))
    }

    type MapStreamStream = ReceiverStream<Result<MapResponse, Status>>;
    async fn map_stream(
        &self,
        request: Request<tonic::Streaming<MapRequest>>,
    ) -> Result<Response<Self::MapStreamStream>, Status> {
        let (mut tx, rx) = mpsc::channel(4);
        map_test_worker(request.into_inner(), tx, self.sender.clone());
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type PushStreamStream = ReceiverStream<Result<PushResponse, Status>>;
    async fn push_stream(
        &self,
        request: Request<tonic::Streaming<PushRequest>>,
    ) -> Result<Response<Self::PushStreamStream>, Status> {
        let (mut tx, rx) = mpsc::channel(4);
        push_worker(request.into_inner(), tx, self.sender.clone());
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_series_reference(
        &self,
        msg: Request<GetSeriesReferenceRequest>,
    ) -> Result<Response<GetSeriesReferenceResponse>, Status> {
        let request = msg.into_inner();
        let (resp_tx, resp_rx) = oneshot::channel();
        let request = WriteRequest::GetReferenceId(GetReferenceId {
            series_id: request.series_id,
            resp: resp_tx,
        });
        if let Err(_) = self.sender.send(request) {
            panic!("Writer thread is dead");
        }
        let response = resp_rx.await.unwrap();
        Ok(Response::new(GetSeriesReferenceResponse {
            series_reference: response,
        }))
    }
}
