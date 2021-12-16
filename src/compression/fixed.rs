use crate::compression::utils::{
    bitpack_256_compress, bitpack_256_decompress, from_zigzag, to_zigzag,
};
use fixed::prelude::*;
use fixed::types::extra::{LeEqU64, Unsigned};
use fixed::FixedI64;
use fixed::types::extra::*;
use std::convert::{TryFrom, TryInto};
use std::mem::size_of;

pub fn compress(data: &[[u8; 8]], buf: &mut Vec<u8>, frac: usize) {
    match frac {
        1 => inner_compress::<U1>(data, buf),
        2 => inner_compress::<U2>(data, buf),
        3 => inner_compress::<U3>(data, buf),
        4 => inner_compress::<U4>(data, buf),
        5 => inner_compress::<U5>(data, buf),
        6 => inner_compress::<U6>(data, buf),
        7 => inner_compress::<U7>(data, buf),
        8 => inner_compress::<U8>(data, buf),
        9 => inner_compress::<U9>(data, buf),
        10 => inner_compress::<U10>(data, buf),
        11 => inner_compress::<U11>(data, buf),
        12 => inner_compress::<U12>(data, buf),
        13 => inner_compress::<U13>(data, buf),
        14 => inner_compress::<U14>(data, buf),
        15 => inner_compress::<U15>(data, buf),
        16 => inner_compress::<U16>(data, buf),
        17 => inner_compress::<U17>(data, buf),
        18 => inner_compress::<U18>(data, buf),
        19 => inner_compress::<U19>(data, buf),
        20 => inner_compress::<U20>(data, buf),
        21 => inner_compress::<U21>(data, buf),
        22 => inner_compress::<U22>(data, buf),
        23 => inner_compress::<U23>(data, buf),
        24 => inner_compress::<U24>(data, buf),
        25 => inner_compress::<U25>(data, buf),
        26 => inner_compress::<U26>(data, buf),
        27 => inner_compress::<U27>(data, buf),
        28 => inner_compress::<U28>(data, buf),
        29 => inner_compress::<U29>(data, buf),
        30 => inner_compress::<U30>(data, buf),
        31 => inner_compress::<U31>(data, buf),
        32 => inner_compress::<U32>(data, buf),
        33 => inner_compress::<U33>(data, buf),
        34 => inner_compress::<U34>(data, buf),
        35 => inner_compress::<U35>(data, buf),
        36 => inner_compress::<U36>(data, buf),
        37 => inner_compress::<U37>(data, buf),
        38 => inner_compress::<U38>(data, buf),
        39 => inner_compress::<U39>(data, buf),
        40 => inner_compress::<U40>(data, buf),
        41 => inner_compress::<U41>(data, buf),
        42 => inner_compress::<U42>(data, buf),
        43 => inner_compress::<U43>(data, buf),
        44 => inner_compress::<U44>(data, buf),
        45 => inner_compress::<U45>(data, buf),
        46 => inner_compress::<U46>(data, buf),
        47 => inner_compress::<U47>(data, buf),
        48 => inner_compress::<U48>(data, buf),
        49 => inner_compress::<U49>(data, buf),
        50 => inner_compress::<U50>(data, buf),
        51 => inner_compress::<U51>(data, buf),
        52 => inner_compress::<U52>(data, buf),
        53 => inner_compress::<U53>(data, buf),
        54 => inner_compress::<U54>(data, buf),
        55 => inner_compress::<U55>(data, buf),
        56 => inner_compress::<U56>(data, buf),
        57 => inner_compress::<U57>(data, buf),
        58 => inner_compress::<U58>(data, buf),
        59 => inner_compress::<U59>(data, buf),
        60 => inner_compress::<U60>(data, buf),
        61 => inner_compress::<U61>(data, buf),
        62 => inner_compress::<U62>(data, buf),
        63 => inner_compress::<U63>(data, buf),
        _ => unimplemented!()
    };
}

pub fn decompress(data: &[u8], buf: &mut Vec<[u8; 8]>, frac: usize) -> (usize, usize) {
    match frac {
        1 => inner_decompress::<U1>(data, buf),
        2 => inner_decompress::<U2>(data, buf),
        3 => inner_decompress::<U3>(data, buf),
        4 => inner_decompress::<U4>(data, buf),
        5 => inner_decompress::<U5>(data, buf),
        6 => inner_decompress::<U6>(data, buf),
        7 => inner_decompress::<U7>(data, buf),
        8 => inner_decompress::<U8>(data, buf),
        9 => inner_decompress::<U9>(data, buf),
        10 => inner_decompress::<U10>(data, buf),
        11 => inner_decompress::<U11>(data, buf),
        12 => inner_decompress::<U12>(data, buf),
        13 => inner_decompress::<U13>(data, buf),
        14 => inner_decompress::<U14>(data, buf),
        15 => inner_decompress::<U15>(data, buf),
        16 => inner_decompress::<U16>(data, buf),
        17 => inner_decompress::<U17>(data, buf),
        18 => inner_decompress::<U18>(data, buf),
        19 => inner_decompress::<U19>(data, buf),
        20 => inner_decompress::<U20>(data, buf),
        21 => inner_decompress::<U21>(data, buf),
        22 => inner_decompress::<U22>(data, buf),
        23 => inner_decompress::<U23>(data, buf),
        24 => inner_decompress::<U24>(data, buf),
        25 => inner_decompress::<U25>(data, buf),
        26 => inner_decompress::<U26>(data, buf),
        27 => inner_decompress::<U27>(data, buf),
        28 => inner_decompress::<U28>(data, buf),
        29 => inner_decompress::<U29>(data, buf),
        30 => inner_decompress::<U30>(data, buf),
        31 => inner_decompress::<U31>(data, buf),
        32 => inner_decompress::<U32>(data, buf),
        33 => inner_decompress::<U33>(data, buf),
        34 => inner_decompress::<U34>(data, buf),
        35 => inner_decompress::<U35>(data, buf),
        36 => inner_decompress::<U36>(data, buf),
        37 => inner_decompress::<U37>(data, buf),
        38 => inner_decompress::<U38>(data, buf),
        39 => inner_decompress::<U39>(data, buf),
        40 => inner_decompress::<U40>(data, buf),
        41 => inner_decompress::<U41>(data, buf),
        42 => inner_decompress::<U42>(data, buf),
        43 => inner_decompress::<U43>(data, buf),
        44 => inner_decompress::<U44>(data, buf),
        45 => inner_decompress::<U45>(data, buf),
        46 => inner_decompress::<U46>(data, buf),
        47 => inner_decompress::<U47>(data, buf),
        48 => inner_decompress::<U48>(data, buf),
        49 => inner_decompress::<U49>(data, buf),
        50 => inner_decompress::<U50>(data, buf),
        51 => inner_decompress::<U51>(data, buf),
        52 => inner_decompress::<U52>(data, buf),
        53 => inner_decompress::<U53>(data, buf),
        54 => inner_decompress::<U54>(data, buf),
        55 => inner_decompress::<U55>(data, buf),
        56 => inner_decompress::<U56>(data, buf),
        57 => inner_decompress::<U57>(data, buf),
        58 => inner_decompress::<U58>(data, buf),
        59 => inner_decompress::<U59>(data, buf),
        60 => inner_decompress::<U60>(data, buf),
        61 => inner_decompress::<U61>(data, buf),
        62 => inner_decompress::<U62>(data, buf),
        63 => inner_decompress::<U63>(data, buf),
        _ => unimplemented!()
    }
}

/// Compresses upto 256 floats into buf
/// Returns the number of bytes written in buf.
/// Panics if buf is too small or data is longer than 256, or the chosen FixedI64<Frac> is too
/// small and overflows
fn inner_compress<Frac: Unsigned + LeEqU64>(data: &[[u8; 8]], buf: &mut Vec<u8>) {
    let mut to_compress: [u32; 256] = [0; 256];
    let mut big_values: [u64; 256] = [0; 256];
    let mut big_idx: [u8; 256] = [0; 256];
    let mut big_count: u8 = 0;

    let first_float = f64::from_be_bytes(data[0]);
    let first = FixedI64::<Frac>::from_num(first_float);
    let mut last = first;
    for i in 1..data.len() {
        let cur = FixedI64::<Frac>::from_num(f64::from_be_bytes(data[i]));
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
    buf.extend_from_slice(&data[0]);

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
fn inner_decompress<Frac: Unsigned + LeEqU64>(data: &[u8], buf: &mut Vec<[u8; 8]>) -> (usize, usize) {
    let mut off = 0;

    // Get len
    let end = off + size_of::<usize>();
    let len = usize::from_be_bytes(<[u8; 8]>::try_from(&data[off..end]).unwrap());
    off = end;

    // Get first value
    let end = off + size_of::<u64>();
    buf.push(data[off..end].try_into().unwrap());
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
    let mut last = FixedI64::<Frac>::from_num(f64::from_be_bytes(buf[0]));
    for i in 1..len {
        let xor = FixedI64::<Frac>::from_bits(from_zigzag(zigzagged[i]));
        let fixed = xor ^ last;
        let val: f64 = fixed.to_num();
        buf.push(val.to_be_bytes());
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
        let mut buf = Vec::new();
        let len = data.len().min(256);
        let v = data[..len].iter().map(|x| x.to_be_bytes()).collect::<Vec<[u8; 8]>>();
        compress(&v, &mut buf, 10);

        let mut res = Vec::new();
        let (b, l) = decompress(&buf[..], &mut res, 10);

        let diff = data[..len]
            .iter()
            .zip(res[..l].iter())
            .map(|(x, y)| (x - f64::from_be_bytes(*y)).abs())
            .fold(f64::NAN, f64::max);

        assert!(0.001 > diff);
        assert_eq!(b, buf.len());
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
