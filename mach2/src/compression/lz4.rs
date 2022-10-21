use crate::{
    constants::SEG_SZ,
    compression::CompressDecompress,
};
use lzzzz::lz4;
use std::convert::TryInto;
use std::slice;
use serde::*;

#[derive(Serialize, Deserialize)]
pub struct LZ4 {}

impl CompressDecompress for LZ4 {
    fn compress(&self, data_len: usize, data: &[[u8; 8]; SEG_SZ], buffer: &mut Vec<u8>) {
        compress(data_len, data, buffer);
    }

    fn decompress(&self, data: &[u8], data_len: &mut usize, buffer: &mut [[u8; 8]; SEG_SZ]) {
        decompress(data, data_len, buffer);
    }
}

fn compress(data_len: usize, data: &[[u8; 8]; SEG_SZ], buffer: &mut Vec<u8>) {

    // unnest from chunks
    let data: &[u8] = unsafe {
        let ptr = data.as_ptr() as *const u8;
        slice::from_raw_parts(ptr, 8 * data_len)
    };

    // write raw data length
    buffer.extend_from_slice(&data_len.to_be_bytes());

    // reserve for size
    let size_offset = buffer.len();
    buffer.extend_from_slice(&0usize.to_be_bytes());

    // compress
    let compress_begin = size_offset + 8;
    let sz = lz4::compress_to_vec(data, buffer, lz4::ACC_LEVEL_DEFAULT).unwrap();
    assert_eq!(buffer.len() - compress_begin, sz);

    // write size
    buffer[size_offset..size_offset + 8].copy_from_slice(&sz.to_be_bytes());
}

fn decompress(data: &[u8], data_len: &mut usize, buffer: &mut [[u8; 8]; SEG_SZ]) {

    // read raw data length
    let dl = usize::from_be_bytes(data[..8].try_into().unwrap());
    *data_len = dl;

    // read compressed size
    let compressed_sz = usize::from_be_bytes(data[8..16].try_into().unwrap());
    let src = &data[16..compressed_sz + 16];

    // unnest from chunks based on data length
    let dst: &mut [u8] = unsafe {
        let ptr = buffer.as_ptr() as *mut u8;
        slice::from_raw_parts_mut(ptr, 8 * dl)
    };

    // decompress
    let sz = lz4::decompress(src, dst).unwrap();
    assert_eq!(sz, dl * 8);
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::{thread_rng, Rng};

    #[test]
    fn test() {
        let mut rng = thread_rng();
        let data: Vec<[u8; 8]> =
            (0..SEG_SZ)
            .map(|_| rng.gen::<usize>().to_be_bytes())
            .collect();
        let mut compressed: Vec<u8> = Vec::new();

        compress(SEG_SZ, data.as_slice().try_into().unwrap(), &mut compressed);

        let mut decompressed: Vec<[u8; 8]> = vec![[0u8; 8]; 256];
        let buf: &mut[[u8;8]] = &mut decompressed[..];
        let mut len: usize = 0;
        decompress(&compressed, &mut len, buf.try_into().unwrap());
        assert_eq!(len, SEG_SZ);
        assert_eq!(data, decompressed);
    }
}


