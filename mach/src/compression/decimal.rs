use crate::compression::utils::{
    bitpack_256_compress, bitpack_256_decompress, float_from_int, float_to_int, from_zigzag,
    to_zigzag,
};
use crate::utils::byte_buffer::ByteBuffer;
use std::convert::{TryFrom, TryInto};
use std::mem::size_of;

pub fn compress(data: &[[u8; 8]], buf: &mut ByteBuffer, precision: u8) {
    //println!("BUF LEN: {}", buf.len());
    let mut to_compress: [u32; 256] = [0; 256];
    let mut big_values: [u64; 256] = [0; 256];
    let mut big_idx: [u8; 256] = [0; 256];
    let mut big_count: u8 = 0;

    let first = float_to_int(precision, f64::from_be_bytes(data[0])).unwrap();

    let mut last = first;
    let mut last_diff = 0;
    for i in 1..data.len() {
        let cur = float_to_int(precision, f64::from_be_bytes(data[i])).unwrap();
        let cur_diff = cur - last;
        let dd = to_zigzag(cur_diff - last_diff);

        if dd < u32::MAX as u64 {
            to_compress[i] = dd as u32;
        } else {
            to_compress[i] = to_compress[i - 1];
            big_values[big_count as usize] = dd;
            big_idx[big_count as usize] = i as u8;
            big_count += 1;
        }

        last = cur;
        last_diff = cur_diff;
    }

    // XOR only starts at the first
    to_compress[0] = to_compress[1];

    // Write precision
    buf.push(precision);

    // Write the length
    //println!("COMPRESS DATA LEN {} at offset {}", data.len(), buf.len());
    buf.extend_from_slice(&data.len().to_be_bytes());

    // Write first item
    buf.extend_from_slice(&first.to_be_bytes());

    // Compress the u32 array
    bitpack_256_compress(buf, &to_compress);

    // Write the zigzag values too big for u32

    // first write the number of values
    buf.push(big_count);

    // then write every value
    for i in 0..big_count as usize {
        buf.extend_from_slice(&big_values[i].to_be_bytes());
        buf.push(big_idx[i]);
    }
}

/// Decompresses data into buf
pub fn decompress(data: &[u8], buf: &mut Vec<[u8; 8]>) {
    // Get precision
    let precision = data[0];

    let mut off = 1;

    // Get len
    let end = off + size_of::<usize>();
    let len = usize::from_be_bytes(<[u8; 8]>::try_from(&data[off..end]).unwrap());
    //println!("LEN: {}", len);
    off = end;

    // Get first value
    let end = off + size_of::<u64>();
    let first_int = i64::from_be_bytes(data[off..end].try_into().unwrap());
    let first = float_from_int(precision, first_int);
    buf.push(first.to_be_bytes());
    off = end;

    // Read bitpacked data
    let mut unpacked = [0u32; 256];
    let read = bitpack_256_decompress(&mut unpacked, &data[off..]);
    off += read;

    // Convert the unpacked values to u64 for unzigging
    let mut zigzagged = [0u64; 256];
    for (i, j) in unpacked.iter().zip(zigzagged.iter_mut()) {
        *j = *i as u64;
    }

    // Read zigzagged u64 values that are too big to have been converted to u32
    let big_count = data[off];
    off += 1;
    for _ in 0..big_count {
        let end = off + size_of::<u64>();
        let value = u64::from_be_bytes(data[off..end].try_into().unwrap());
        off = end;
        let idx = data[off];
        off += 1;
        zigzagged[idx as usize] = value;
    }

    // Finally, perform the unrolling
    let mut last = first_int;
    let mut last_diff = 0;
    for i in 1..len {
        let dd = from_zigzag(zigzagged[i]);
        let cur_diff = dd + last_diff;
        let cur = cur_diff + last;

        let val = float_from_int(precision, cur);
        buf.push(val.to_be_bytes());
        last = cur;
        last_diff = cur_diff;
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::*;
    use fixed::types::extra::U10;

    fn compress_decompress(data: &[f64]) {
        let mut buf = vec![0u8; 4096];
        let mut byte_buf = ByteBuffer::new(&mut buf[..]);
        let len = data.len().min(256);
        let v = data[..len]
            .iter()
            .map(|x| x.to_be_bytes())
            .collect::<Vec<[u8; 8]>>();
        compress(&v, &mut byte_buf, 3);
        let sz = byte_buf.len();

        let mut res = Vec::new();
        decompress(&buf[..sz], &mut res);
        let l = res.len();

        let diff = data[..len]
            .iter()
            .zip(res[..l].iter())
            .map(|(x, y)| (x - f64::from_be_bytes(*y)).abs())
            .fold(f64::NAN, f64::max);

        //println!("DIFF {}", diff);
        assert!(diff <= 0.001);
        //assert_eq!(b, sz);
        assert_eq!(l, len);
    }

    #[test]
    fn test_compress_decompress_random() {
        use rand::Fill;
        let mut data: [f64; 256] = [0.0; 256];
        (&mut data[..]).try_fill(&mut rand::thread_rng()).unwrap();
        compress_decompress(&data[..]);
    }

    #[test]
    fn test_compress_decompress() {
        for (_, data) in UNIVARIATE_DATA.iter() {
            let raw = data.iter().map(|x| x.values[0]).collect::<Vec<f64>>();
            for lo in (0..raw.len()).step_by(256) {
                let hi = raw.len().min(lo + 256);
                compress_decompress(&raw[lo..hi]);
            }
        }
    }
}
