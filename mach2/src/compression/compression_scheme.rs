use crate::{
    byte_buffer::ByteBuffer,
    compression::{delta_of_delta::DeltaOfDelta, lz4::LZ4, CompressDecompress},
    segment::SegmentArray,
};
use serde::*;

#[derive(Serialize, Deserialize)]
pub enum CompressionScheme {
    LZ4(LZ4),
    DeltaOfDelta(DeltaOfDelta),
}

impl CompressionScheme {
    pub fn compress(&self, len: usize, data: &SegmentArray, buffer: &mut ByteBuffer) {
        match self {
            Self::LZ4(l) => l.compress(len, data, buffer),
            Self::DeltaOfDelta(l) => l.compress(len, data, buffer),
        };
    }

    pub fn decompress(&self, data: &[u8], len: &mut usize, buffer: &mut SegmentArray) {
        match self {
            Self::LZ4(l) => l.decompress(data, len, buffer),
            Self::DeltaOfDelta(l) => l.decompress(data, len, buffer),
        };
    }
}

//pub struct Compress
