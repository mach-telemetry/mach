mod timestamps;
mod fixed;
mod xor;
mod utils;

const MAGIC: &str = "202107280428";

pub enum Compression {
}

impl Compression {
    pub fn compress(&self, ts: &[u64], values: &[&[[u8; 8]]], buf: &mut Vec<u8>) {
    }
}
