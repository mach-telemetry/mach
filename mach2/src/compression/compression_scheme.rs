use crate:: {
    constants::SEG_SZ,
    compression::{
        CompressDecompress,
        lz4::LZ4,
        delta_of_delta::DeltaOfDelta,
    },
    segment::SegmentArray,
    byte_buffer::ByteBuffer,
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
            Self::DeltaOfDelta(l) => l.compress(len, data, buffer)
        };
    }

    pub fn decompress(&self, data: &[u8], len: &mut usize, buffer: &mut SegmentArray) {
        match self {
            Self::LZ4(l) => l.decompress(data, len, buffer),
            Self::DeltaOfDelta(l) => l.decompress(data, len, buffer)
        };
    }
}

//pub struct Compress

