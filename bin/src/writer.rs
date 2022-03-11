use tonic::{transport::Server, Request, Response, Status};

pub mod mach_rpc {
    tonic::include_proto!("mach_rpc"); // The string specified here must match the proto package name
}

use mach::{
    compression::{CompressFn, Compression},
    persistent_list::VectorBackend,
    series::{SeriesConfig, Types},
    tags::Tags,
    tsdb::Mach,
    writer::Writer,
};
use mach_rpc::writer_service_server::{WriterService, WriterServiceServer};
use mach_rpc::{EchoRequest, EchoResponse, MapRequest, MapResponse};
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
}

struct GetReferenceId {
    series_id: u64,
    resp: oneshot::Sender<u64>,
}

struct Push {
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
    println!("Launcing worker");
    let counter = Arc::new(AtomicUsize::new(0));
    counter_watcher(counter.clone()).await;
    while let Some(item) = channel.recv().await {
        match item {
            WriteRequest::GetReferenceId(x) => {
                x.resp.send(0);
            }
            WriteRequest::Push(x) => {
                let response: HashMap<u64, bool> =
                    x.samples.iter().map(|(k, _)| (*k, true)).collect();
                let len = response.len();
                x.resp.send(response);
                counter.fetch_add(len, SeqCst);
            }
        }
    }
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
        let mut in_stream = request.into_inner();
        let (mut tx, rx) = mpsc::channel(4);
        let sender_to_writer = self.sender.clone();

        tokio::spawn(async move {
            while let Some(result) = in_stream.next().await {
                match result {
                    Ok(v) => {
                        let (resp_tx, resp_rx) = oneshot::channel();

                        let request = WriteRequest::Push(Push {
                            samples: v.samples,
                            resp: resp_tx,
                        });
                        if let Err(_) = sender_to_writer.send(request) {
                            panic!("Writer thread is dead");
                        }
                        let response = resp_rx.await.unwrap();
                        tx.send(Ok(MapResponse {
                            samples: response.into(),
                        }))
                        .await
                        .expect("working rx")
                    }
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
}
