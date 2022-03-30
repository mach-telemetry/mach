pub mod mach_rpc {
    tonic::include_proto!("mach_rpc"); // The string specified here must match the proto package name
}

use mach_rpc::tsdb_service_client::TsdbServiceClient;
use mach_rpc::MapRequest;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering::SeqCst},
    Arc,
};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{channel, Sender};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::transport::Channel;

//fn echo_requests_iter() -> impl Stream<Item = EchoRequest> {
//    tokio_stream::iter(1..usize::MAX).map(|i| EchoRequest {
//        msg: format!("msg {:02}", i),
//    })
//}

//async fn echo_stream(
//    client: &mut TsdbServiceClient<Channel>,
//    counter: Arc<AtomicUsize>,
//    num: usize,
//) {
//    let in_stream = echo_requests_iter().take(num);
//
//    let response = client.echo_stream(in_stream).await.unwrap();
//
//    let mut resp_stream = response.into_inner();
//
//    let mut instant = Instant::now();
//    while let Some(recieved) = resp_stream.next().await {
//        let received = recieved.unwrap();
//        let count = counter.fetch_add(1, SeqCst);
//        //if count > 0 && count % 1_000_000 == 0 {
//        //    let elapsed = instant.elapsed();
//        //    println!("received 1,000,000 responses in {:?}", elapsed);
//        //    instant = Instant::now();
//        //}
//    }
//}

//async fn map(client: &mut TsdbServiceClient<Channel>, counter: Arc<AtomicUsize>) {
//    let (tx, mut rx) = channel(1);
//    tokio::spawn(map_maker(tx));
//
//    loop {
//        client.map(rx.recv().await.unwrap()).await.unwrap();
//        counter.fetch_add(100_000, SeqCst);
//    }
//}

async fn map_stream(client: &mut TsdbServiceClient<Channel>, counter: Arc<AtomicUsize>) {
    let (tx, rx) = channel(1);

    let in_stream = ReceiverStream::new(rx);
    let response = client.map_stream(in_stream).await.unwrap();
    tokio::spawn(map_maker(tx));

    let mut resp_stream = response.into_inner();

    let mut _instant = Instant::now();
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

async fn map_maker(sender: Sender<MapRequest>) {
    let mut map = HashMap::new();
    for i in 0..1 {
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

    let clients = 1;
    let mut v = Vec::new();
    for _ in 0..clients {
        let counter = counter.clone();
        v.push(tokio::spawn(async move {
            let mut client = TsdbServiceClient::connect("http://127.0.0.1:50050")
                .await
                .unwrap();
            map_stream(&mut client, counter.clone()).await;
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
