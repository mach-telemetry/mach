mod bytes_server;
use bytes_server::*;

struct BytesToString { }

impl BytesHandler for BytesToString {
    fn handle_bytes(&self, bytes: Option<Vec<u8>>) -> Result<Option<Vec<u8>>, Status> {
        let item = String::from_utf8(bytes.unwrap()).unwrap();
        println!("item {}", item);
        Ok(None)
    }
}

#[tokio::main]
async fn main() {
    let mut client = BytesClient::new("https://ip-172-31-22-116.ec2.internal:50051").await;
    let bytes: Vec<u8> = "hello world".as_bytes().into();
    client.send(Some(bytes)).await;
}

