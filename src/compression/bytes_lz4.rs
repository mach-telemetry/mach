use crate::segment::FullSegment;
use crate::utils::byte_buffer::ByteBuffer;
use lzzzz::lz4;
use std::convert::TryInto;
use crate::compression::Error;
use crate::sample::Bytes;

pub fn bytes_lz4_compress(segment: &[[u8; 8]], buf: &mut ByteBuffer) {
    let mut to_compress = Vec::new();

    for item in segment.iter() {
        let ptr = usize::from_be_bytes(*item) as *const u8;
        let bytes = unsafe { Bytes::from_raw(ptr) };
        to_compress.extend_from_slice(bytes.as_raw_bytes());
        // Drop bytes and free underlying memory
    }

    // Write in the length
    buf.extend_from_slice(&segment.len().to_be_bytes());

    // Reserve the first 8 bytes for the sz
    let mut b = buf.unused();
    let csz = lz4::compress(to_compress.as_slice(), &mut b[8..], 1).unwrap();
    b[..8].clone_from_slice(&csz.to_be_bytes());
}

/// Decompresses data into buf
/// Returns the number of bytes read from data and number of items decompressed.
/// Panics if buf is not long enough.
pub fn bytes_lz4_decompress(data: &[u8], buf: &mut Vec<[u8; 8]>) -> (usize, usize) {
    let mut off = 0;
    let usz = std::mem::size_of::<usize>();
    let end = usz;

    let len = usize::from_be_bytes(data[off..end].try_into().unwrap());
    off += usz;

    let end = off + usz;
    let raw_sz = usize::from_be_bytes(data[off..end].try_into().unwrap());
    off += usz;

    let mut bytes = vec![0u8; raw_sz as usize].into_boxed_slice();
    lz4::decompress(&data[off..off + raw_sz], &mut bytes[..]).unwrap();
    off += raw_sz;

    let mut start = 0;
    for _ in 0..len {
        let (bytes, bytes_sz) = Bytes::from_raw_bytes(&bytes[start..]);
        buf.push((bytes.into_raw() as usize).to_be_bytes());
        start += bytes_sz;
    }

    (off, len)
}
