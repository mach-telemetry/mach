// Copyright (c) 2023 Franco Solleza, Intel Corporation, Brown University
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

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

