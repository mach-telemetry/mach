use crate::utils::byte_buffer::ByteBuffer;
use lzzzz::lz4;
use std::convert::TryInto;
use crate::compression::delta_of_delta;

pub fn compress(_len: usize, indexes: &[[u8; 8]], to_compress: &[u8], byte_buf: &mut ByteBuffer) {
    //let start = buf.len();

    let buf = byte_buf.unused();

    // write offsets
    buf[0..8].copy_from_slice(&0u64.to_be_bytes()); // offset for compressed index
    buf[8..16].copy_from_slice(&0u64.to_be_bytes()); // compressed index size
    buf[16..24].copy_from_slice(&0u64.to_be_bytes()); // offset for compressed data
    buf[24..32].copy_from_slice(&0u64.to_be_bytes()); // compressed data size
    buf[32..40].copy_from_slice(&to_compress.len().to_be_bytes()); // raw_data size

    // Compress index
    //let compressed_idx_offset = buf.len();
    let compressed_idx_sz = {
        let mut byte_buffer = ByteBuffer::new(&mut buf[40..]);
        delta_of_delta::compress(indexes, &mut byte_buffer);
        byte_buffer.len()
    };
    buf[0..8].copy_from_slice(&40usize.to_be_bytes());
    buf[8..16].copy_from_slice(&compressed_idx_sz.to_be_bytes());

    // Compress data
    let compressed_data_offset = 40 + compressed_idx_sz;
    let compressed_data_sz = lz4::compress(to_compress, &mut buf[compressed_data_offset..], 1).unwrap();
    buf[16..24].copy_from_slice(&compressed_data_offset.to_be_bytes());
    buf[24..32].copy_from_slice(&compressed_data_sz.to_be_bytes());

    byte_buf.add_len(compressed_data_offset + compressed_data_sz);
}

/// Decompresses data into buf
pub fn decompress(data: &[u8], buf: &mut Vec<[u8; 8]>, bytes: &mut Vec<u8>) {

    //let usz = std::mem::size_of::<usize>();

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
