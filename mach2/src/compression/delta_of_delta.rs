use crate::compression::timestamps;
use crate::compression::CompressDecompress;
use crate::constants::SEG_SZ;
use crate::segment::SegmentArray;
use crate::byte_buffer::ByteBuffer;
use serde::*;

#[derive(Serialize, Deserialize)]
pub struct DeltaOfDelta { }

impl CompressDecompress for DeltaOfDelta {
    fn compress(&self, len: usize, data: &SegmentArray, buffer: &mut ByteBuffer) {
        let data: Vec<u64> = data.iter().map(|x| u64::from_be_bytes(*x)).collect();
        let data_slice: &[u64; 256] = data.as_slice().try_into().unwrap();
        timestamps::compress(len, data_slice, buffer);
    }

    /// Decompresses data into buf
    fn decompress(&self, data: &[u8], data_len: &mut usize, buffer: &mut SegmentArray) {
        let mut v = vec![0u64; SEG_SZ];
        let v_buf = v.as_mut_slice().try_into().unwrap();
        timestamps::decompress(data, data_len, v_buf);
        for (x, y) in v.iter().zip(buffer.iter_mut()) {
            *y = x.to_be_bytes();
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::{Rng, thread_rng};

    #[test]
    fn compress_decompress() {
        let mut rng = thread_rng();

        let increments: Vec<u64> = (0..256).map(|_| rng.gen::<u64>() % 100).collect();
        let mut integers: Vec<[u8; 8]> = Vec::new();
        let mut sum: u64 = rng.gen::<u64>() % 1_000;
        for x in increments {
            integers.push(sum.to_be_bytes());
            sum += x;
        }

        assert_eq!(integers.len(), 256);
        let mut compressed_bytes = vec![0u8; 1_000_000];
        let mut byte_buffer = ByteBuffer::new(0, compressed_bytes.as_mut_slice());
        let to_compress: &SegmentArray = integers.as_slice().try_into().unwrap();
        let dod = DeltaOfDelta{};
        dod.compress(256, to_compress, &mut byte_buffer);

        let mut len = 0;
        let mut decompressed: Vec<[u8; 8]> = vec![[0u8; 8]; 256];
        let decompress_buffer: &mut[[u8; 8]; 256] = decompressed.as_mut_slice().try_into().unwrap();
        dod.decompress(byte_buffer.as_slice(), &mut len, decompress_buffer);

        assert_eq!(len, 256);
        assert_eq!(integers.as_slice(), &decompress_buffer[..]);
    }
}


