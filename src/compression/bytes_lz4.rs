use crate::compression::Error;
use crate::sample::Bytes;
use crate::segment::FullSegment;
use crate::utils::byte_buffer::ByteBuffer;
use lzzzz::{lz4, lz4_hc};
use std::convert::TryInto;

pub fn compress(segment: &[[u8; 8]], buf: &mut ByteBuffer) {
    let mut to_compress = Vec::new();

    for item in segment.iter() {
        let ptr = usize::from_be_bytes(*item) as *const u8;
        let bytes = unsafe { Bytes::from_raw(ptr) };
        to_compress.extend_from_slice(bytes.as_raw_bytes());
        bytes.into_raw(); // Prevent bytes from dropping
    }

    let start = buf.len();
    // Write in the length of the segment
    buf.extend_from_slice(&segment.len().to_be_bytes());

    // Write total size of bytes
    buf.extend_from_slice(&to_compress.len().to_be_bytes());

    // Place holder for compressed size
    let csz_offset = buf.len();
    buf.extend_from_slice(&0u64.to_be_bytes());

    // Reserve the first 8 bytes for the sz
    let mut b = buf.unused();
    let csz = lz4::compress(to_compress.as_slice(), &mut b[..], 1).unwrap();
    drop(b);
    buf.add_len(csz);

    // Fill the place holder
    buf.as_mut_slice()[csz_offset..csz_offset + 8].copy_from_slice(&csz.to_be_bytes());
}

/// Decompresses data into buf
pub fn decompress(data: &[u8], buf: &mut Vec<[u8; 8]>) {
    let mut off = 0;
    let usz = std::mem::size_of::<usize>();

    let end = usz;
    let len = usize::from_be_bytes(data[off..end].try_into().unwrap());
    off += usz;

    let end = off + usz;
    let bytes_sz = usize::from_be_bytes(data[off..end].try_into().unwrap());
    off += usz;

    let end = off + usz;
    let raw_sz = usize::from_be_bytes(data[off..end].try_into().unwrap());
    off += usz;

    let mut bytes = vec![0u8; bytes_sz as usize].into_boxed_slice();
    lz4::decompress(&data[off..off + raw_sz], &mut bytes[..]).unwrap();
    off += raw_sz;

    let mut start = 0;
    for _ in 0..len {
        let (bytes, bytes_sz) = Bytes::from_raw_bytes(&bytes[start..]);
        buf.push((bytes.into_raw() as usize).to_be_bytes());
        start += bytes_sz;
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::LOG_DATA;
    use std::convert::TryInto;

    fn compress_decompress(data: &[String]) {
        let mut v = Vec::new();
        for s in data.iter() {
            let ptr = Bytes::from_slice(s.as_bytes()).into_raw();
            v.push((ptr as u64).to_be_bytes());
        }
        let mut buf = vec![0u8; 8192];
        let mut byte_buf = ByteBuffer::new(&mut buf[..]);
        compress(&v[..], &mut byte_buf);
        let mut results = Vec::new();
        decompress(&buf[..], &mut results);

        for (result, exp) in results.iter().zip(data.iter()) {
            let ptr = usize::from_be_bytes(*result) as *const u8;
            let bytes = unsafe { Bytes::from_raw(ptr) };
            let s = std::str::from_utf8(bytes.bytes()).unwrap();
            assert_eq!(s, exp);
        }
    }

    #[test]
    fn test_logs() {
        let logs = &*LOG_DATA;
        for (idx, chunk) in logs.as_slice().chunks(256).enumerate() {
            compress_decompress(chunk);
            //println!("{}", chunk.len());
        }
    }
}
