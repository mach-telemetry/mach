mod bytes_server;

struct BytesToString { }

impl bytes_server::BytesHandler for BytesToString {
    fn handle_bytes(&self, bytes: Option<Vec<u8>>) -> Result<Option<Vec<u8>>, Status> {
        let item = String::from(bytes.unwrap().as_slice());
        println!("item {}", item);
        Ok(None)
    }
}

fn main() {
}

