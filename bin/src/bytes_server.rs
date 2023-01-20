// Copyright (c) 2023 Franco Solleza, Intel Corporation, Brown University
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

pub use tonic::Status;
use tonic::{transport::Server, Request, Response};
pub mod bytes_service {
    tonic::include_proto!("bytes_service");
}
use bytes_service::bytes_service_client::BytesServiceClient;
use bytes_service::bytes_service_server::{BytesService, BytesServiceServer};
use bytes_service::BytesMessage;
use tokio::runtime::Runtime;
//use std::net::SocketAddr;

struct ResultWrapper(Result<Option<Vec<u8>>, Status>);
type BytesResponse = Result<Response<BytesMessage>, Status>;

impl std::convert::From<ResultWrapper> for BytesResponse {
    fn from(r: ResultWrapper) -> Self {
        Ok(Response::new(BytesMessage { data: r.0? }))
    }
}

pub trait BytesHandler: Sync + Send + 'static {
    fn handle_bytes(&self, bytes: Option<Vec<u8>>) -> Result<Option<Vec<u8>>, Status>;
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct BytesServer<B: BytesHandler> {
    handler: B,
}

impl<B: BytesHandler> BytesServer<B> {
    pub fn new(handler: B) -> Self {
        Self { handler }
    }

    pub fn serve(self) {
        let local_ip = local_ip_address::local_ip().unwrap();
        let addr = std::net::SocketAddr::new(local_ip, 50051);
        println!("Serving on: {:?}", local_ip);
        let rt = Runtime::new().expect("failed to obtain a new RunTime object");
        let server_future = Server::builder()
            .add_service(BytesServiceServer::new(self))
            .serve(addr);
        rt.block_on(server_future)
            .expect("failed to successfully run the future on RunTime");
    }
}

#[tonic::async_trait]
impl<B: BytesHandler> BytesService for BytesServer<B> {
    async fn send(&self, request: Request<BytesMessage>) -> Result<Response<BytesMessage>, Status> {
        ResultWrapper(self.handler.handle_bytes(request.into_inner().data)).into()
    }
}

#[allow(dead_code)]
pub struct BytesClient(BytesServiceClient<tonic::transport::Channel>);

impl BytesClient {
    #[allow(dead_code)]
    pub async fn new(addr: &'static str) -> Self {
        let client = BytesServiceClient::connect(addr).await.unwrap();
        Self(client)
    }

    #[allow(dead_code)]
    pub async fn send(&mut self, data: Option<Vec<u8>>) -> Option<Vec<u8>> {
        let request = tonic::Request::new(BytesMessage { data });
        let response = self.0.send(request).await.unwrap();
        response.into_inner().data
    }
}
