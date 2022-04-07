use tokio::sync::RwLock;
use tonic::{transport::Server, Request, Response, Status};

pub mod mach_rpc {
    tonic::include_proto!("mach_rpc"); // The string specified here must match the proto package name
}

use mach_rpc::tsdb_service_server::{TsdbService, TsdbServiceServer};
use mach_rpc::{
    queue_config, AddSeriesRequest, AddSeriesResponse, EchoRequest, EchoResponse, MapRequest,
    MapResponse, ReadSeriesRequest, ReadSeriesResponse,
};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};

mod writer;
#[allow(unused_imports)]
use mach::durable_queue::{FileConfig, KafkaConfig};
use mach::{
    compression::{CompressFn, Compression},
    durable_queue::QueueConfig,
    id::SeriesId,
    series::{SeriesConfig, Types},
    tags::Tags,
    tsdb::Mach,
    utils::random_id,
    writer::WriterConfig,
};
use std::{collections::HashMap, sync::Arc};

impl mach_rpc::QueueConfig {
    fn from_mach(config: QueueConfig) -> Self {
        mach_rpc::QueueConfig {
            configs: match config {
                QueueConfig::Kafka(cfg) => {
                    Some(queue_config::Configs::Kafka(mach_rpc::KafkaConfig {
                        bootstrap: cfg.bootstrap,
                        topic: cfg.topic,
                    }))
                }
                QueueConfig::File(cfg) => Some(queue_config::Configs::File(mach_rpc::FileConfig {
                    dir: cfg.dir.into_os_string().into_string().unwrap(),
                    file: cfg.file,
                })),
            },
        }
    }
}

pub struct MachTSDB {
    tsdb: Arc<RwLock<Mach>>,
    writers: HashMap<String, String>,
}

impl MachTSDB {
    fn new() -> Self {
        let mut mach = Mach::new();
        let mut writers = HashMap::new();
        for i in 0..1 {
            //let queue_config = FileConfig {
            //    dir: CONF.out_path.clone(),
            //    file: random_id(),
            //}.config();
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
            let id = writer.id().0;
            let addr = format!("127.0.0.1:500{}", 51 + i);
            writer::serve_writer(writer, &addr);
            writers.insert(id, addr);
        }
        Self {
            tsdb: Arc::new(RwLock::new(mach)),
            writers,
        }
    }
}

#[tonic::async_trait]
impl TsdbService for MachTSDB {
    async fn echo(&self, msg: Request<EchoRequest>) -> Result<Response<EchoResponse>, Status> {
        //println!("Got a request: {:?}", msg);
        let reply = EchoResponse {
            msg: format!("Echo: {}", msg.into_inner().msg),
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
        let reply = MapResponse { samples: response };
        Ok(Response::new(reply))
    }

    async fn add_series(
        &self,
        msg: Request<AddSeriesRequest>,
    ) -> Result<Response<AddSeriesResponse>, Status> {
        let req = msg.into_inner();
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
        let tags = Tags::from(req.tags);

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
        let mut tsdb_locked = self.tsdb.write().await;
        let (writer_id, series_id) = tsdb_locked.add_series(conf).unwrap();
        let writer_address = self.writers.get(&writer_id.0).unwrap().clone();
        let series_id = series_id.0;

        Ok(Response::new(AddSeriesResponse {
            writer_address,
            series_id,
        }))
    }

    async fn read(
        &self,
        req: Request<ReadSeriesRequest>,
    ) -> Result<Response<ReadSeriesResponse>, Status> {
        let req = req.into_inner();
        let serid = SeriesId(req.series_id);

        let tsdb_locked = self.tsdb.read().await;
        let r = tsdb_locked.read(serid).await;

        let response_queue = mach_rpc::QueueConfig::from_mach(r.response_queue);
        let data_queue = mach_rpc::QueueConfig::from_mach(r.data_queue);

        Ok(Response::new(ReadSeriesResponse {
            response_queue: Some(response_queue),
            data_queue: Some(data_queue),
            offset: r.offset,
        }))
    }

    type EchoStreamStream = ReceiverStream<Result<EchoResponse, Status>>;
    async fn echo_stream(
        &self,
        request: Request<tonic::Streaming<EchoRequest>>,
    ) -> Result<Response<Self::EchoStreamStream>, Status> {
        let mut in_stream = request.into_inner();
        let (tx, rx) = mpsc::channel(4);
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
        println!("Setting up stream");
        let mut in_stream = request.into_inner();
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            while let Some(result) = in_stream.next().await {
                match result {
                    Ok(v) => {
                        let response: HashMap<u64, bool> =
                            v.samples.iter().map(|(k, _)| (*k, true)).collect();
                        tx.send(Ok(MapResponse { samples: response }))
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
