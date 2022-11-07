mod bytes_server;
use bytes_server::*;

struct BytesToString {}

impl BytesHandler for BytesToString {
    fn handle_bytes(&self, bytes: Option<Vec<u8>>) -> Result<Option<Vec<u8>>, Status> {
        let item = String::from_utf8(bytes.unwrap()).unwrap();
        println!("item {}", item);
        Ok(None)
    }
}

fn main() {
    bytes_server::BytesServer::new(BytesToString {}).serve("172.31.22.116:50051");
}
