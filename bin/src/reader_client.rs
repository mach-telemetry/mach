pub mod mach_rpc {
    tonic::include_proto!("mach_rpc"); // The string specified here must match the proto package name
}

use futures::stream::Stream;
use mach_rpc::tsdb_service_client::TsdbServiceClient;
use mach_rpc::writer_service_client::WriterServiceClient;
use mach_rpc::{
    add_series_request::ValueType, value::PbType, AddSeriesRequest, AddSeriesResponse, EchoRequest,
    EchoResponse, GetSeriesReferenceRequest, GetSeriesReferenceResponse, MapRequest, MapResponse,
    PushRequest, PushResponse, Sample, Value,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicUsize, Ordering::SeqCst},
    Arc,
};
use std::thread::sleep;
use std::time::Duration;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::transport::Channel;

const exp_duration: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = TsdbServiceClient::connect("http://127.0.0.1:50050")
        .await
        .unwrap();

    let req = AddSeriesRequest {
        types: vec![ValueType::F64.into(), ValueType::Bytes.into()],
        tags: [(String::from("hello"), String::from("world"))
            .into_iter()
            .collect(),
    };
    let AddSeriesResponse {
        writer_address,
        series_id,
    } = client.add_series(req).await.unwrap().into_inner();

    let writer_address = format!("http://{}", writer_address);
    let mut writer_client = WriterServiceClient::connect(writer_address).await.unwrap();

    thread::spawn(push_data);
    thread::spawn(pull_data);
}
