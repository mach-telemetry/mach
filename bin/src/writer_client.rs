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
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::transport::Channel;

fn echo_requests_iter() -> impl Stream<Item = EchoRequest> {
    tokio_stream::iter(1..usize::MAX).map(|i| EchoRequest {
        msg: format!("msg {:02}", i),
    })
}

async fn map_stream(client: &mut WriterServiceClient<Channel>, counter: Arc<AtomicUsize>) {
    let (tx, rx) = channel(1);
    tokio::spawn(map_maker(tx));

    let in_stream = ReceiverStream::new(rx);
    let response = client.map_stream(in_stream).await.unwrap();

    let mut resp_stream = response.into_inner();

    let mut instant = Instant::now();
    while let Some(recieved) = resp_stream.next().await {
        let received = recieved.unwrap();
        let count = counter.fetch_add(100_000, SeqCst);
        //if count > 0 && count % 1_000_000 == 0 {
        //    let elapsed = instant.elapsed();
        //    println!("received 1,000,000 responses in {:?}", elapsed);
        //    instant = Instant::now();
        //}
    }
}

async fn map_maker(sender: Sender<MapRequest>) {
    let mut map = HashMap::new();
    for i in 0..100_000 {
        map.insert(i, i);
    }
    loop {
        sender
            .send(MapRequest {
                samples: map.clone().into(),
            })
            .await
            .unwrap();
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let counter = Arc::new(AtomicUsize::new(0));
    let mut client = TsdbServiceClient::connect("http://127.0.0.1:50050")
        .await
        .unwrap();

    // Timeseries information
    let mut tags: HashMap<String, String> = HashMap::new();
    tags.insert("foo".into(), "bar".into());
    let types = vec![ValueType::F64.into(), ValueType::Bytes.into()];
    let req = AddSeriesRequest { types, tags };
    let AddSeriesResponse {
        writer_address,
        series_id,
    } = client.add_series(req).await.unwrap().into_inner();
    println!("Writer address: {:?}", writer_address);
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

    //let mut counter: u64 = 0;
    //let mut instant = Instant::now();
    //loop {
    //    let request = tonic::Request::new(EchoRequest {
    //        msg: format!("Tonic{}", counter)
    //    });
    //    let response = client.echo(request).await?;
    //    //println!("RESPONSE={:?}", response.into_inner().msg);
    //    counter += 1;
    //    if counter % 1_000 == 0 {
    //        let elapsed = instant.elapsed();
    //        println!("1,000,000 requests in {:?}", elapsed);
    //        instant = Instant::now();
    //    }
    //    //sleep(Duration::from_secs(1));
    //}
    Ok(())
}
