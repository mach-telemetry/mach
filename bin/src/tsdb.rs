use tonic::{transport::Server, Request, Response, Status};

pub mod mach_rpc {
    tonic::include_proto!("mach_rpc"); // The string specified here must match the proto package name
}

use mach_rpc::tsdb_service_server::{TsdbService, TsdbServiceServer};
use mach_rpc::writer_service_server::{WriterService, WriterServiceServer};
use mach_rpc::{
    AddSeriesRequest, AddSeriesResponse, EchoRequest, EchoResponse, MapRequest, MapResponse,
};
use std::{error::Error, io::ErrorKind, net::ToSocketAddrs, pin::Pin, time::Duration};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};

//use futures::Stream;

mod writer;
use mach::{
    compression::{CompressFn, Compression},
    persistent_list::VectorBackend,
    series::{SeriesConfig, Types},
    tags::Tags,
    tsdb::Mach,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

pub struct MachTSDB {
    tsdb: Arc<Mutex<Mach<VectorBackend>>>,
}

impl MachTSDB {
    fn new() -> Self {
        let mut mach = Mach::new();

        writer::serve_writer(mach.new_writer().unwrap(), "[::1]:50051");

        Self {
            tsdb: Arc::new(Mutex::new(mach)),
        }
    }
}

#[tonic::async_trait]
impl TsdbService for MachTSDB {
    async fn echo(&self, msg: Request<EchoRequest>) -> Result<Response<EchoResponse>, Status> {
        //println!("Got a request: {:?}", msg);
        let reply = EchoResponse {
            msg: format!("Echo: {}", msg.into_inner().msg).into(),
        };
        Ok(Response::new(reply))
    }

    async fn map(&self, msg: Request<MapRequest>) -> Result<Response<MapResponse>, Status> {
        //println!("Got a request: {:?}", msg);
        let response: HashMap<u64, bool> = msg
            .into_inner()
            .samples
            .iter()
            .map(|(k, _)| (*k, true))
            .collect();
        let reply = MapResponse {
            samples: response.into(),
        };
        Ok(Response::new(reply))
    }

    async fn add_series(
        &self,
        msg: Request<AddSeriesRequest>,
    ) -> Result<Response<AddSeriesResponse>, Status> {
        let mut req = msg.into_inner();
        let mut map = HashMap::new();
        for kv in req.tags.drain(..) {
            map.insert(kv.key, kv.value);
        }
        let tags = Tags::from(map);
        let types: Vec<Types> = req
            .types
            .iter()
            .map(|x| match x {
                0 => Types::U64,
                1 => Types::F64,
                2 => Types::Bytes,
                _ => unimplemented!(),
            })
            .collect();

        let compression = {
            let compression: Vec<CompressFn> = types
                .iter()
                .map(|x| match x {
                    Types::F64 => CompressFn::Decimal(3),
                    Types::Bytes => CompressFn::BytesLZ4,
                    _ => unimplemented!(),
                })
                .collect();
            Compression::from(compression)
        };

        let nvars = types.len();
        let seg_count = 1;

        let conf = SeriesConfig {
            tags,
            types,
            compression,
            seg_count,
            nvars,
        };

        //self.tsdb.lock().add_series(conf);

        unimplemented!();
    }

    type EchoStreamStream = ReceiverStream<Result<EchoResponse, Status>>;
    async fn echo_stream(
        &self,
        request: Request<tonic::Streaming<EchoRequest>>,
    ) -> Result<Response<Self::EchoStreamStream>, Status> {
        let mut in_stream = request.into_inner();
        let (mut tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            while let Some(result) = in_stream.next().await {
                match result {
                    Ok(v) => tx
                        .send(Ok(EchoResponse { msg: v.msg }))
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

    type MapStreamStream = ReceiverStream<Result<MapResponse, Status>>;
    async fn map_stream(
        &self,
        request: Request<tonic::Streaming<MapRequest>>,
    ) -> Result<Response<Self::MapStreamStream>, Status> {
        let mut in_stream = request.into_inner();
        let (mut tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            while let Some(result) = in_stream.next().await {
                match result {
                    Ok(v) => {
                        let response: HashMap<u64, bool> =
                            v.samples.iter().map(|(k, _)| (*k, true)).collect();
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50050".parse()?;
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
