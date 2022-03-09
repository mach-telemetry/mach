pub mod mach_server {
    tonic::include_proto!("mach_server"); // The string specified here must match the proto package name
}

use mach_server::mach_client::MachClient;
use mach_server::{AddSeriesRequest, AddSeriesResponse, EchoRequest, EchoResponse};
use std::time::Duration;
use std::thread::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = MachClient::connect("http://[::1]:50051").await?;

    let mut counter: u64 = 0;
    loop {
        let request = tonic::Request::new(EchoRequest {
            msg: format!("Tonic{}", counter)
        });
        let response = client.echo(request).await?;
        println!("RESPONSE={:?}", response.into_inner().msg);
        counter += 1;
        sleep(Duration::from_secs(1));
    }
    Ok(())
}

