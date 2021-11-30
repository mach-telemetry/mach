use crate::compression::utils::{
    bitpack_256_compress, bitpack_256_decompress, from_zigzag, to_zigzag,
};
use fixed::prelude::*;
use fixed::types::extra::{LeEqU64, Unsigned};
use fixed::FixedI64;
use std::mem::size_of;
use std::convert::{TryFrom, TryInto};

/// Compresses upto 256 floats into buf
/// Returns the number of bytes written in buf.
/// Panics if buf is too small or data is longer than 256, or the chosen FixedI64<Frac> is too
/// small and overflows
fn compress<Frac: Unsigned + LeEqU64>(data: &[f64], buf: &mut Vec<u8>) {
    let mut to_compress: [u32; 256] = [0; 256];
    let mut big_values: [u64; 256] = [0; 256];
    let mut big_idx: [u8; 256] = [0; 256];
    let mut big_count: u8 = 0;

    let first = FixedI64::<Frac>::from_num(data[0]);
    let mut last = first;
    for i in 1..data.len() {
        let cur = FixedI64::<Frac>::from_num(data[i]);
        let xor = to_zigzag(last.to_bits() ^ cur.to_bits());

        if xor < u32::MAX as u64 {
            to_compress[i] = xor as u32;
        } else {
            to_compress[i] = to_compress[i - 1];
            big_values[big_count as usize] = xor;
            big_idx[big_count as usize] = i as u8;
            big_count += 1;
        }
        last = cur;
    }
    // XOR only starts at the first
    to_compress[0] = to_compress[1];

    // Write the length
    buf.extend_from_slice(&data.len().to_be_bytes());

    // Write first item
    buf.extend_from_slice(&data[0].to_be_bytes());

    // Compress the u32 array
    let sz = bitpack_256_compress(buf, &to_compress);

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
/// Returns the number of bytes read from data and number of items decompressed.
/// Panics if buf is not long enough.
fn decompress<Frac: Unsigned + LeEqU64>(data: &[u8], buf: &mut [f64]) -> (usize, usize) {
    let mut off = 0;

    // Get len
    let end = off + size_of::<usize>();
    let len = usize::from_be_bytes(<[u8; 8]>::try_from(&data[off..end]).unwrap());
    off = end;

    // Get first value
    let end = off + size_of::<u64>();
    buf[0] = f64::from_be_bytes(<[u8; 8]>::try_from(&data[off..end]).unwrap());
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
    let mut last = FixedI64::<Frac>::from_num(buf[0]);
    for i in 1..len {
        let xor = FixedI64::<Frac>::from_bits(from_zigzag(zigzagged[i]));
        let fixed = xor ^ last;
        buf[i] = fixed.to_num();
        last = fixed;
    }

    (off, len)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::*;
    use fixed::types::extra::U10;

    fn compress_decompress(data: &[f64]) {
        let mut buf = [0u8; 8192];
        let len = data.len().min(256);
        let bytes = compress::<U10>(&data[..len], &mut buf[..]);

        let mut res = [0f64; 256];
        let (b, l) = decompress::<U10>(&buf[..bytes], &mut res[..]);

        let diff = data[..len]
                .iter()
                .zip(res[..l].iter())
                .map(|(x, y)| (x - y).abs())
                .fold(f64::NAN, f64::max);

        assert!(0.001 > diff);
        assert_eq!(b, bytes);
        assert_eq!(l, len);
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
