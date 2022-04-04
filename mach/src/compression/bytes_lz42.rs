use crate::sample::Bytes;
use crate::utils::byte_buffer::ByteBuffer;
use lzzzz::lz4;
use std::convert::TryInto;

pub fn compress(len: usize, to_compress: &[u8], buf: &mut ByteBuffer) {
    //let start = buf.len();
    // Write in the length of the segment
    buf.extend_from_slice(&len.to_be_bytes());

    // Write total size of bytes
    buf.extend_from_slice(&to_compress.len().to_be_bytes());

    // Place holder for compressed size
    let csz_offset = buf.len();
    buf.extend_from_slice(&0u64.to_be_bytes());

    // Reserve the first 8 bytes for the sz
    let b = buf.unused();
    let csz = lz4::compress(to_compress, &mut *b, 1).unwrap();
    //drop(b);
    buf.add_len(csz);

    // Fill the place holder
    buf.as_mut_slice()[csz_offset..csz_offset + 8].copy_from_slice(&csz.to_be_bytes());
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

    let end = off + usz;
    let raw_sz = usize::from_be_bytes(data[off..end].try_into().unwrap());
    off += usz;

    bytes.resize(bytes_sz, 0u8);
    lz4::decompress(&data[off..off + raw_sz], &mut bytes[..]).unwrap();
    off += raw_sz;
    let _off = off;

    let mut start = 0;
    for _ in 0..len {
        let ptr: *const u8 = bytes[start..].as_ptr();
        let b = unsafe { Bytes::from_raw(ptr) };
        let b_sz = b.as_raw_bytes().len();
        buf.push((b.into_raw() as usize).to_be_bytes());
        start += b_sz;
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::LOG_DATA;

    fn compress_decompress(data: &[String]) {
        let mut v = Vec::new();
        for s in data.iter() {
            let b = Bytes::from_slice(s.as_bytes());
            v.extend_from_slice(b.as_raw_bytes());
        }
        let mut buf = vec![0u8; 8192];
        let mut byte_buf = ByteBuffer::new(&mut buf[..]);
        compress(data.len(), &v[..], &mut byte_buf);
        let mut results = Vec::new();
        let mut heap = Vec::new();
        decompress(&buf[..], &mut results, &mut heap);

        for (result, exp) in results.iter().zip(data.iter()) {
            let ptr = usize::from_be_bytes(*result) as *const u8;
            let bytes = unsafe { Bytes::from_raw(ptr) };
            let s = std::str::from_utf8(bytes.bytes()).unwrap();
            assert_eq!(s, exp);
            bytes.into_raw(); // don't deallocate
        }
    }

    #[test]
    fn test_logs() {
        let logs = &*LOG_DATA;
        for (_idx, chunk) in logs.as_slice().chunks(256).enumerate() {
            compress_decompress(chunk);
            //println!("{}", chunk.len());
        }
    }
}
