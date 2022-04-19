pub mod mach_rpc {
    tonic::include_proto!("mach_rpc"); // The string specified here must match the proto package name
}

use mach_rpc::tsdb_service_client::TsdbServiceClient;
use mach_rpc::writer_service_client::WriterServiceClient;
use mach_rpc::{
    add_series_request::ValueType, value::PbType, AddSeriesRequest, AddSeriesResponse,
    GetSeriesReferenceRequest, GetSeriesReferenceResponse, MapRequest, PushRequest, Sample, Value,
};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering::SeqCst},
    Arc,
};
use std::time::Duration;
use tokio::sync::mpsc::{channel, Sender};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::transport::Channel;
use rand::Rng;
use std::time::{SystemTime, UNIX_EPOCH};

//fn echo_requests_iter() -> impl Stream<Item = EchoRequest> {
//    tokio_stream::iter(1..usize::MAX).map(|i| EchoRequest {
//        msg: format!("msg {:02}", i),
//    })
//}

#[allow(dead_code)]
async fn map_stream(client: &mut WriterServiceClient<Channel>, counter: Arc<AtomicUsize>) {
    let (tx, rx) = channel(1);
    tokio::spawn(map_maker(tx));

    let in_stream = ReceiverStream::new(rx);
    let response = client.map_stream(in_stream).await.unwrap();

    let mut resp_stream = response.into_inner();

    //let mut instant = Instant::now();
    while let Some(recieved) = resp_stream.next().await {
        let _received = recieved.unwrap();
        let _count = counter.fetch_add(100_000, SeqCst);
        //if count > 0 && count % 1_000_000 == 0 {
        //    let elapsed = instant.elapsed();
        //    println!("received 1,000,000 responses in {:?}", elapsed);
        //    instant = Instant::now();
        //}
    }
}

#[allow(dead_code)]
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

async fn counter_watcher(counter: Arc<AtomicUsize>) {
    tokio::spawn(async move {
        let mut last = 0;
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let cur = counter.load(SeqCst);
            println!("received {} / sec", cur - last);
            last = cur;
        }
    });
}

async fn sample_maker(sender: Sender<PushRequest>) {
    //let mut rng = rand::thread_rng();
    let mut counter = 0;
    loop {
        let mut samples = Vec::new();
        for _ in 0..10000 {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            let timestamp: u64 = now.as_millis().try_into().unwrap();
            let value: f64 = 12345.0;
            let refid: u64 = counter % 100;
            let sample = Sample {
                id: counter,
                refid,
                timestamp: timestamp,
                values: vec![
                    Value {
                        pb_type: Some(PbType::F64(value)),
                    },
                ],
            };
            samples.push(sample);
            counter += 1;
        }
        let push_request = PushRequest { sample: samples };
        sender.send(push_request).await.unwrap();
    }
}

async fn sample_stream(mut client: WriterServiceClient<Channel>, counter: Arc<AtomicUsize>) {
    let (tx, rx) = channel(1);
    tokio::spawn(sample_maker(tx));

    let in_stream = ReceiverStream::new(rx);
    let response = client.push_stream(in_stream).await.unwrap();

    let mut resp_stream = response.into_inner();
    while let Some(recieved) = resp_stream.next().await {
        let received = recieved.unwrap();
        let _count = counter.fetch_add(received.responses.len(), SeqCst);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();
    tokio::task::spawn(counter_watcher(counter_clone));
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

    /* Streaming */
    let mut handles = Vec::new();
    for i in 0..2 {
        let mut writer_client = WriterServiceClient::connect(writer_address.clone()).await.unwrap();
        let counter_clone = counter.clone();
        handles.push(tokio::task::spawn(sample_stream(writer_client, counter_clone)));
    }
    for handle in handles {
        handle.await;
    }

    /* A single push*/
    // let mut writer_client = WriterServiceClient::connect(writer_address).await.unwrap();
    // let series_ref_request = GetSeriesReferenceRequest { series_id };
    // let GetSeriesReferenceResponse { series_reference } = writer_client
    //     .get_series_reference(series_ref_request)
    //     .await
    //     .unwrap()
    //     .into_inner();
    // println!("Series reference: {:?}", series_reference);

    // // Samples
    // let sample = Sample {
    //     timestamp: 12345,
    //     values: vec![
    //         Value {
    //             pb_type: Some(PbType::F64(123.4)),
    //         },
    //         Value {
    //             pb_type: Some(PbType::Str("hello world".into())),
    //         },
    //     ],
    // };
    // //samples.insert(series_reference, sample);
    // let request = PushRequest { id: 0, refid: series_reference, sample: Some(sample) };

    // let result = writer_client
    //     .push(request)
    //     .await
    //     .unwrap()
    //     .into_inner()
    //     .result;
    // println!("{:?}", result);

    Ok(())
}
