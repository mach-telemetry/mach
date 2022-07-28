use crate::sample::Bytes;
use crate::utils::byte_buffer::ByteBuffer;
use lzzzz::lz4;
use std::convert::TryInto;
use crate::compression::delta_of_delta;

pub fn compress(len: usize, indexes: &[[u8; 8]], to_compress: &[u8], buf: &mut ByteBuffer) {
    //let start = buf.len();

    // write offsets
    buf.extend_from_slice(&0u64.to_be_bytes()); // offset for compressed index
    buf.extend_from_slice(&0u64.to_be_bytes()); // compressed index size
    buf.extend_from_slice(&0u64.to_be_bytes()); // offset for compressed data
    buf.extend_from_slice(&0u64.to_be_bytes()); // compressed data size
    buf.extend_from_slice(&to_compress.len().to_be_bytes()); // raw data size

    // Compress index
    let compressed_idx_offset = buf.len();
    delta_of_delta::compress(indexes, buf);
    let compressed_idx_sz = buf.len() - compressed_idx_offset;
    buf.as_mut_slice()[0..8].copy_from_slice(&compressed_idx_offset.to_be_bytes());
    buf.as_mut_slice()[8..16].copy_from_slice(&compressed_idx_sz.to_be_bytes());

    // Compress data
    let compressed_data_offset = buf.len();
    let b = buf.unused();
    let csz = lz4::compress(to_compress, &mut *b, 1).unwrap();
    buf.add_len(csz);
    buf.as_mut_slice()[16..24].copy_from_slice(&compressed_data_offset.to_be_bytes());
    buf.as_mut_slice()[24..32].copy_from_slice(&csz.to_be_bytes());
}

/// Decompresses data into buf
pub fn decompress(data: &[u8], buf: &mut Vec<[u8; 8]>, bytes: &mut Vec<u8>) {

    let usz = std::mem::size_of::<usize>();

    let compressed_idx_offset = u64::from_be_bytes(data[0..8].try_into().unwrap());
    let compressed_idx_sz = u64::from_be_bytes(data[8..16].try_into().unwrap());
    let compressed_data_offset = u64::from_be_bytes(data[16..24].try_into().unwrap());
    let compressed_data_sz = u64::from_be_bytes(data[24..32].try_into().unwrap());
    let raw_data_sz = u64::from_be_bytes(data[32..40].try_into().unwrap());

    // decompress indexes
    let start = compressed_idx_offset as usize;
    let end = start + compressed_idx_sz as usize;
    delta_of_delta::decompress(&data[start..end], buf);

    bytes.resize(raw_data_sz as usize, 0u8);
    let start = compressed_data_offset as usize;
    let end = start + compressed_data_sz as usize;
    lz4::decompress(&data[start..end], &mut bytes[..]).unwrap();
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::LOG_DATA;

    fn compress_decompress(data: &[String]) {
        let mut v = Vec::new();
        let mut indexes = Vec::new();
        for s in data.iter() {
            let b = Bytes::from_slice(s.as_bytes());
            indexes.push(v.len().to_be_bytes());
            v.extend_from_slice(b.as_raw_bytes());
        }
        let mut buf = vec![0u8; 8192];
        let mut byte_buf = ByteBuffer::new(&mut buf[..]);
        compress(data.len(), &indexes, &v[..], &mut byte_buf);
        let mut results = Vec::new();
        let mut heap = Vec::new();
        decompress(&buf[..], &mut results, &mut heap);

        for (result, exp) in results.iter().zip(data.iter()) {
            let idx = usize::from_be_bytes(*result);
            let bytes = unsafe { Bytes::from_raw(heap[idx..].as_ptr()) };
            //let ptr = usize::from_be_bytes(*result) as *const u8;
            //let bytes = unsafe { Bytes::from_raw(ptr) };
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
