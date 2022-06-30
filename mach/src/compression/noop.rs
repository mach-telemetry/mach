use crate::sample::Bytes;
use crate::utils::byte_buffer::ByteBuffer;
use std::convert::TryInto;

pub fn compress(len: usize, to_compress: &[u8], buf: &mut ByteBuffer) {
    //let start = buf.len();
    // Write in the length of the segment
    buf.extend_from_slice(&len.to_be_bytes());

    // Write total size of bytes
    buf.extend_from_slice(&to_compress.len().to_be_bytes());

    buf.extend_from_slice(to_compress);

    // Place holder for compressed size
    //let csz_offset = buf.len();
    //buf.extend_from_slice(&0u64.to_be_bytes());

    // Reserve the first 8 bytes for the sz
    //let b = buf.unused();
    //let csz = lz4::compress(to_compress, &mut *b, 1).unwrap();
    ////drop(b);
    //buf.add_len(csz);

    // Fill the place holder
    //buf.as_mut_slice()[csz_offset..csz_offset + 8].copy_from_slice(&csz.to_be_bytes());
}

/// Decompresses data into buf
pub fn decompress(data: &[u8], buf: &mut Vec<[u8; 8]>, bytes: &mut Vec<u8>) {
    let mut off = 0;
    let usz = std::mem::size_of::<usize>();

    let end = usz;
    let len = usize::from_be_bytes(data[off..end].try_into().unwrap());
    off += usz;

    let end = off + usz;
    let bytes_sz = usize::from_be_bytes(data[off..end].try_into().unwrap());
    off += usz;

    bytes.extend_from_slice(&data[off..off+bytes_sz]);

    let mut start = 0;
    for _ in 0..len {
        let ptr: *const u8 = bytes[start..].as_ptr();
        let b = unsafe { Bytes::from_raw(ptr) };
        let b_sz = b.as_raw_bytes().len();
        buf.push((b.into_raw() as usize).to_be_bytes());
        start += b_sz;
    }
}

