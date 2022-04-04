pub mod mach_rpc {
    tonic::include_proto!("mach_rpc"); // The string specified here must match the proto package name
}

use futures::stream::Stream;
use mach_rpc::tsdb_service_client::TsdbServiceClient;
use mach_rpc::writer_service_client::WriterServiceClient;
use mach_rpc::{
    add_series_request::ValueType, value::PbType, AddSeriesRequest, AddSeriesResponse, EchoRequest,
    EchoResponse, GetSeriesReferenceRequest, GetSeriesReferenceResponse, MapRequest, MapResponse,
    PushRequest, PushResponse, ReadSeriesRequest, ReadSeriesResponse, Sample, Value,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicUsize, Ordering::SeqCst},
    Arc,
};
use std::thread::sleep;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::transport::Channel;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = TsdbServiceClient::connect("http://127.0.0.1:50050")
        .await
        .unwrap();

    let req = AddSeriesRequest {
        types: vec![ValueType::F64.into(), ValueType::Bytes.into()],
        tags: [(String::from("hello"), String::from("world"))]
            .into_iter()
            .collect(),
    };
    let AddSeriesResponse {
        writer_address,
        series_id,
    } = client.add_series(req).await.unwrap().into_inner();

    let writer_address = format!("http://{}", writer_address);
    let mut writer_client = WriterServiceClient::connect(writer_address).await.unwrap();

    let series_ref_request = GetSeriesReferenceRequest { series_id };
    let GetSeriesReferenceResponse { series_reference } = writer_client
        .get_series_reference(series_ref_request)
        .await
        .unwrap()
        .into_inner();
    println!("Series reference: {:?}", series_reference);

    // Samples
    let mut samples = HashMap::new();
    let sample = Sample {
        timestamp: 12345,
        values: vec![
            Value {
                pb_type: Some(PbType::F64(123.4)),
            },
            Value {
                pb_type: Some(PbType::Str("hello world".into())),
            },
        ],
    };
    samples.insert(series_reference, sample);
    let request = PushRequest { samples };

    let results = writer_client
        .push(request)
        .await
        .unwrap()
        .into_inner()
        .results;
    println!("{:?}", results);

    // Read snapshot
    let r = client.read(ReadSeriesRequest { series_id }).await.unwrap();

    Ok(())
}
