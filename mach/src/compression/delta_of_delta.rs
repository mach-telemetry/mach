use crate::compression::utils::{
    bitpack_256_compress, bitpack_256_decompress, float_from_int, float_to_int, from_zigzag,
    to_zigzag,
};
use crate::utils::byte_buffer::ByteBuffer;
use crate::compression::timestamps;
use std::convert::{TryFrom, TryInto};
use std::mem::size_of;

pub fn compress(data: &[[u8; 8]], buf: &mut ByteBuffer) {
    let data: Vec<u64> = data.iter().map(|x| u64::from_be_bytes(*x)).collect();
    timestamps::compress(data.as_slice(), buf);
}

/// Decompresses data into buf
pub fn decompress(data: &[u8], buf: &mut Vec<[u8; 8]>) {
    let mut v = Vec::new();
    timestamps::decompress(data, &mut v);
    for item in v.iter() {
        buf.push(item.to_be_bytes());
    }
}


