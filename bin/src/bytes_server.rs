use tonic::{transport::Server, Request, Response};
pub use tonic::Status;
pub mod bytes_service {
    tonic::include_proto!("bytes_service");
}
use bytes_service::bytes_service_server::{BytesService, BytesServiceServer};
use bytes_service::{BytesMessage};
use tokio::runtime::Runtime;

struct ResultWrapper(Result<Option<Vec<u8>>, Status>);
type BytesResponse = Result<Response<BytesMessage>, Status>;

impl std::convert::Into<BytesResponse> for ResultWrapper {
    fn into(self) -> BytesResponse {
        let data = self.0?;
        Ok(Response::new(BytesMessage { data }))
    }
}

pub trait BytesHandler: Sync + Send + 'static {
    fn handle_bytes(&self, bytes: Option<Vec<u8>>) -> Result<Option<Vec<u8>>, Status>;
}

#[derive(Debug)]
pub struct BytesServer<B: BytesHandler> {
    handler: B
}

impl<B: BytesHandler> BytesServer<B> {
    pub fn new(handler: B) -> Self {
        Self { handler }
    }

    pub fn serve(self) {
        let addr = "[::1]:50051".parse().unwrap();
        let mut rt = Runtime::new().expect("failed to obtain a new RunTime object");
        let server_future = Server::builder()
                            .add_service(BytesServiceServer::new(self))
                            .serve(addr);
        rt.block_on(server_future).expect("failed to successfully run the future on RunTime");
    }
}

#[tonic::async_trait]
impl<B: BytesHandler> BytesService for BytesServer<B> {
    async fn send(&self, request: Request<BytesMessage>) -> Result<Response<BytesMessage>, Status> {
        ResultWrapper(self.handler.handle_bytes(request.into_inner().data)).into()
    }
}
