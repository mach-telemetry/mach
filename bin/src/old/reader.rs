use mach::reader::ReadServer;
use mach::{durable_queue::QueueConfig, id::SeriesId};
use tonic::{transport::Server, Request, Response, Status};
pub mod mach_rpc {
    tonic::include_proto!("mach_rpc");
}
use mach_rpc::{
    queue_config,
    reader_service_server::{ReaderService, ReaderServiceServer},
    ReadSeriesRequest, ReadSeriesResponse,
};

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

pub fn serve_reader(read_server: ReadServer, addr: &str) {
    println!("Serving the read server at {}", addr);
    tokio::spawn(
        Server::builder()
            .add_service(ReaderServiceServer::new(MachReadServer::new(read_server)))
            .serve(addr.parse().unwrap()),
    );
}

pub struct MachReadServer {
    inner: ReadServer,
}

impl MachReadServer {
    pub fn new(reader: ReadServer) -> Self {
        Self { inner: reader }
    }
}

#[tonic::async_trait]
impl ReaderService for MachReadServer {
    async fn read(
        &self,
        req: Request<ReadSeriesRequest>,
    ) -> Result<Response<ReadSeriesResponse>, Status> {
        let req = req.into_inner();
        let serid = SeriesId(req.series_id);

        let r = self.inner.read_request(serid).await;

        let response_queue = mach_rpc::QueueConfig::from_mach(r.response_queue);
        let data_queue = mach_rpc::QueueConfig::from_mach(r.data_queue);

        Ok(Response::new(ReadSeriesResponse {
            response_queue: Some(response_queue),
            data_queue: Some(data_queue),
            offset: r.offset,
        }))
    }
}
