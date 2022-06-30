use crate::utils::byte_buffer::ByteBuffer;
use std::convert::{TryInto};
use lzzzz::{lz4};

pub fn compress(data: &[[u8; 8]], buf: &mut ByteBuffer) {
    let len = data.len();

    // length of the segment
    buf.extend_from_slice(&len.to_be_bytes());

    // reserve length of the segment
    let csz_offset = buf.len();
    buf.extend_from_slice(&0usize.to_be_bytes());

    let bytes_len = len * 8;
    let ptr = data.as_ptr() as *const u8;
    let data: &[u8] = unsafe { std::slice::from_raw_parts(ptr, bytes_len) };

    let b = buf.unused();
    let csz = lz4::compress(data, &mut *b, 1).unwrap();
    //drop(b);
    let _l = buf.len();
    buf.add_len(csz);

    //let mut decompress_buf: Vec<u8> = Vec::with_capacity(1_000_000_000);
    //lz4::decompress(&buf.as_mut_slice()[l..l + csz], &mut decompress_buf[..]).unwrap();

    // Fill the place holder
    buf.as_mut_slice()[csz_offset..csz_offset + 8].copy_from_slice(&csz.to_be_bytes());
}

/// Decompresses data into buf
pub fn decompress(data: &[u8], buf: &mut Vec<[u8; 8]>) {

    let len = usize::from_be_bytes(data[..8].try_into().unwrap());
    let csz = usize::from_be_bytes(data[8..16].try_into().unwrap());

    let mut decompress_buf: Vec<u8> = vec![0u8; len * 8];
    lz4::decompress(&data[16..16 + csz], &mut decompress_buf[..]).unwrap();

    let ptr = decompress_buf.as_slice().as_ptr() as *const [u8; 8];
    let slice: &[[u8; 8]] = unsafe { std::slice::from_raw_parts(ptr, len) };
    buf.extend_from_slice(slice);
}


#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::*;

    #[test]
    fn compress_decompress_values() {
        let data = UNIVARIATE_DATA.clone();
        let mut buf = vec![0u8; 4096];
        for (_id, samples) in data[..].iter().enumerate() {
            let raw = samples.1.iter().map(|x| x.values[0]).collect::<Vec<f64>>();
            for lo in (0..raw.len()).step_by(256) {
                let mut byte_buf = ByteBuffer::new(&mut buf[..]);
                let hi = raw.len().min(lo + 256);
                let exp: Vec<[u8; 8]> = raw[lo..hi].iter().map(|x| x.to_be_bytes()).collect();
                let mut res = Vec::new();
                let _bytes = compress(exp.as_slice(), &mut byte_buf);
                let sz = byte_buf.len();
                decompress(&buf[..sz], &mut res);
                assert_eq!(res, exp);
            }
        }
    }
}



