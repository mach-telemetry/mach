use crate::utils::byte_buffer::ByteBuffer;
use crate::compression::timestamps;

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


