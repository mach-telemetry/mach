mod timestamps;
mod fixed;
mod xor;
mod utils;

use crate::segment::FullSegment;

const MAGIC: &str = "202107280428";

pub enum Compression {
}

impl Compression {
    pub fn compress(&self, segment: &FullSegment, buf: &mut Vec<u8>) {
    }
}
