use tonic::{Request, Response, Status};

use crate::increment_sample_counter;
use crate::rpc;
use rpc::tsdb_service_server::TsdbService;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};

#[derive(Clone)]
pub struct NoneTSDB {}

impl NoneTSDB {
    pub fn new() -> Self {
        NoneTSDB {}
    }

    async fn push_stream_handler(
        &self,
        response_channel: mpsc::Sender<Result<rpc::PushResponse, Status>>,
        mut request_stream: tonic::Streaming<rpc::PushRequest>,
    ) {
        while let Some(Ok(mut request)) = request_stream.next().await {
            let mut responses = Vec::new();
            for samples in request.samples.drain(..) {
                for sample in samples.samples {
                    responses.push(rpc::SampleResult {
                        id: sample.id,
                        result: true,
                    })
                }
            }
            increment_sample_counter(responses.len());
            response_channel
                .send(Ok(rpc::PushResponse { responses }))
                .await
                .unwrap();
        }
    }
}

#[tonic::async_trait]
impl TsdbService for NoneTSDB {
    async fn echo(
        &self,
        msg: Request<rpc::EchoRequest>,
    ) -> Result<Response<rpc::EchoResponse>, Status> {
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
        _msg: Request<rpc::ReadRequest>,
    ) -> Result<Response<rpc::ReadResponse>, Status> {
        unimplemented!();
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
