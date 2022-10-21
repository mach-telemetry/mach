pub mod compression_scheme;
pub mod lz4;
pub mod timestamps;

use crate::constants::SEG_SZ;

pub trait CompressDecompress {
    fn compress(&self, data_len: usize, data: &[[u8; 8]; SEG_SZ], buffer: &mut Vec<u8>);
    fn decompress(&self, data: &[u8], data_len: &mut usize, buffer: &mut [[u8; 8]; SEG_SZ]);
}

