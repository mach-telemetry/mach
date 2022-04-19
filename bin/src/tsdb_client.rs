pub mod mach_rpc {
    tonic::include_proto!("mach_rpc"); // The string specified here must match the proto package name
}

use mach_rpc::tsdb_service_client::TsdbServiceClient;
use mach_rpc::{self as rpc, MapRequest};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering::SeqCst},
    Arc,
};
use std::time::{SystemTime, Duration, Instant, UNIX_EPOCH};
use tokio::sync::mpsc::{channel, Sender};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::transport::Channel;

async fn sample_maker(sender: Sender<rpc::PushRequest>) {
    //let mut rng = rand::thread_rng();
    let mut counter = 0;
    loop {
        let mut request = Vec::new();
        for t in ["a", "b", "c"] {
            let mut samples = Vec::new();
            let mut tags = HashMap::new();
            tags.insert(String::from(t), String::from(t));
            for _ in 0..1000 {
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
                let timestamp: u64 = now.as_millis().try_into().unwrap();
                let value: f64 = 12345.0;
                let sample = rpc::Sample {
                    id: counter,
                    timestamp: timestamp,
                    values: vec![
                        rpc::Value {
                            value_type: Some(rpc::value::ValueType::F64(value)),
                        },
                    ],
                };
                samples.push(sample);
                counter += 1;
            }
            request.push(rpc::Samples { tags, samples} );
        }
        let push_request = rpc::PushRequest { samples: request };
        sender.send(push_request).await.unwrap();
    }
}

async fn sample_stream(mut client: TsdbServiceClient<Channel>, counter: Arc<AtomicUsize>) {
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

    let clients = 1;
    let mut v = Vec::new();
    for _ in 0..clients {
        let counter = counter.clone();
        v.push(tokio::spawn(async move {
            let mut client = TsdbServiceClient::connect("http://127.0.0.1:50050")
                .await
                .unwrap();
            sample_stream(client, counter.clone()).await;
        }));
    }
    let mut last = 0;
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let cur = counter.load(SeqCst);
        println!("received {} / sec", cur - last);
        last = cur;
    }

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
}
