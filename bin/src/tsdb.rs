use tonic::{transport::Server, Request, Response, Status};

pub mod mach_server {
    tonic::include_proto!("mach_server"); // The string specified here must match the proto package name
}

use mach_server::mach_server::{Mach, MachServer};
use mach_server::{AddSeriesRequest, AddSeriesResponse, EchoRequest, EchoResponse};

#[derive(Default, Debug)]
pub struct MachGlobalState {}

#[tonic::async_trait]
impl Mach for MachGlobalState {
    async fn echo(&self, msg: Request<EchoRequest>) -> Result<Response<EchoResponse>, Status> {
        println!("Got a request: {:?}", msg);
        let reply = EchoResponse {
            msg: format!("Echo: {}", msg.into_inner().msg).into(),
        };
        Ok(Response::new(reply))
    }

    async fn add_series(
        &self,
        msg: Request<AddSeriesRequest>,
    ) -> Result<Response<AddSeriesResponse>, Status> {
        unimplemented!()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let tsdb = MachGlobalState::default();

    Server::builder()
        .add_service(MachServer::new(tsdb))
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
