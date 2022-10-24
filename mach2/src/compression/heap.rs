use crate::byte_buffer::ByteBuffer;
use lzzzz::lz4;

pub fn compress_heap(data: &[u8], buffer: &mut ByteBuffer) {

    // write raw data length
    buffer.extend_from_slice(&data.len().to_be_bytes());

    // reserve for size
    let size_offset = buffer.len();
    buffer.extend_from_slice(&0usize.to_be_bytes());

    // compress
    let compress_begin = size_offset + 8;
    let sz = lz4::compress(data, buffer.remaining(), lz4::ACC_LEVEL_DEFAULT).unwrap();
    buffer.set_len(buffer.len() + sz);

    // write size
    buffer.as_mut_slice()[size_offset..size_offset + 8].copy_from_slice(&sz.to_be_bytes());
}

pub fn decompress_heap(data: &[u8], data_len: &mut usize, dst: &mut [u8]) {

    // read raw data length
    let dl = usize::from_be_bytes(data[..8].try_into().unwrap());
    *data_len = dl;

    // read compressed size
    let compressed_sz = usize::from_be_bytes(data[8..16].try_into().unwrap());
    let src = &data[16..compressed_sz + 16];

    // decompress
    let sz = lz4::decompress(src, dst).unwrap();
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::{Rng, thread_rng, distributions::{Alphanumeric, DistString}};
    use crate::byte_buffer::ByteBuffer;

    #[test]
    fn test() {

        let mut rng = thread_rng();
        let expected_string: String = Alphanumeric.sample_string(&mut rng, 750_000);
        let mut buf = vec![0u8; 1_000_000];
        let mut byte_buf = ByteBuffer::new(0, buf.as_mut_slice());
        compress_heap(expected_string.as_bytes(), &mut byte_buf);

        let mut result_string = vec![0u8; 1_000_000];
        let mut len = 0;
        decompress_heap(byte_buf.as_slice(), &mut len, result_string.as_mut_slice());
        assert_eq!(len, expected_string.as_bytes().len());
        assert_eq!(expected_string.as_bytes(), &result_string[..len]);

    }
}

