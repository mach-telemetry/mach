use crate:: {
    constants::SEG_SZ,
    compression::{CompressDecompress, lz4::LZ4},
};
use serde::*;

#[derive(Serialize, Deserialize)]
pub enum CompressionScheme {
    LZ4(LZ4),
}

impl CompressionScheme {
    pub fn compress(&self, len: usize, data: &[[u8; 8]; SEG_SZ], buffer: &mut Vec<u8>) {
        match self {
            Self::LZ4(l) => l.compress(len, data, buffer)
        };
    }

    pub fn decompress(&self, data: &[u8], len: &mut usize, buffer: &mut [[u8; 8]; SEG_SZ]) {
        match self {
            Self::LZ4(l) => l.decompress(data, len, buffer)
        };
    }
}

//pub struct Compress

