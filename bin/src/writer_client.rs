pub mod mach_rpc {
    tonic::include_proto!("mach_rpc"); // The string specified here must match the proto package name
}

use mach_rpc::writer_service_client::WriterServiceClient;
use mach_rpc::{AddSeriesRequest, AddSeriesResponse, EchoRequest, EchoResponse, MapRequest, MapResponse};
use std::time::{Instant, Duration};
use std::thread::sleep;
use tonic::transport::Channel;
use futures::stream::Stream;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering::SeqCst}};
use tokio::sync::mpsc::{Sender, Receiver, channel};
use std::collections::HashMap;

fn echo_requests_iter() -> impl Stream<Item = EchoRequest> {
    tokio_stream::iter(1..usize::MAX).map(|i| EchoRequest {
        msg: format!("msg {:02}", i),
    })
}

async fn map_stream(client: &mut WriterServiceClient<Channel>, counter: Arc<AtomicUsize>) {
    let (tx, rx) = channel(1);
    tokio::spawn(map_maker(tx));

    let in_stream = ReceiverStream::new(rx);
    let response = client
        .map_stream(in_stream)
        .await
        .unwrap();

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
        sender.send(MapRequest { samples: map.clone().into() }).await.unwrap();
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let counter = Arc::new(AtomicUsize::new(0));

    let clients = 4;
    let mut v = Vec::new();
    for i in 0..clients {
        let counter = counter.clone();
        v.push(tokio::spawn(async move {
            let mut client = WriterServiceClient::connect("http://[::1]:50051").await.unwrap();
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
    Ok(())
}

