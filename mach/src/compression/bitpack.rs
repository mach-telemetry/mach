use crate::compression::utils::{
    bitpack_256_compress, bitpack_256_decompress
};
use crate::utils::byte_buffer::ByteBuffer;
use std::convert::{TryInto};

pub fn compress(data: &[[u8; 8]], buf: &mut ByteBuffer) {
    let mut to_compress = [0u32; 256];
    data.iter().enumerate().for_each(|(i, x)| {
        let u = u64::from_be_bytes(*x);
        let x = match u.try_into() {
            Ok(x) => x,
            Err(e) => panic!("{} {:?}", u, e),
        };
        to_compress[i] = x;
    });
    buf.extend_from_slice(&(data.len().to_be_bytes()));
    bitpack_256_compress(buf, &to_compress);
}

/// Decompresses data into buf
pub fn decompress(data: &[u8], buf: &mut Vec<[u8; 8]>) {
    let count = usize::from_be_bytes(data[..8].try_into().unwrap());
    let mut unpacked = [0u32; 256];
    let _read = bitpack_256_decompress(&mut unpacked, &data[8..]);
    unpacked[..count].iter().copied().for_each(|x| buf.push((x as u64).to_be_bytes()));
}

