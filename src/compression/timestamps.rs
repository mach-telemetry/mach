use crate::compression::utils::{
    bitpack_256_compress, bitpack_256_decompress, from_zigzag, to_zigzag, ByteBuffer
};
use std::{
    convert::{TryFrom, TryInto},
    mem::size_of,
};

/// Compresses upto 256 timestamps into buf
/// Returns the number of bytes written in buf.
/// Panics if buf is too small or timestamps is longer than 256
pub fn compress(timestamps: &[u64], buf: &mut ByteBuffer) {
    let len = timestamps.len();
    assert!(len <= 256);

    // Some scratch pad and locations to remember items too big too store in u32
    let mut to_compress: [u32; 256] = [0; 256];
    let mut big_value: [u64; 256] = [0; 256];
    let mut big_offset: [u8; 256] = [0; 256];
    let mut big_count: u8 = 0;

    let mut last_diff: u64 = timestamps[1] - timestamps[0];
    for i in 2..len {
        let diff = timestamps[i] - timestamps[i - 1];

        // we don't expect this to fail
        let (a, b): (i64, i64) = (diff.try_into().unwrap(), last_diff.try_into().unwrap());
        let zz = to_zigzag(a - b);

        // but this one might because we go from u64 -> u32
        if zz < u32::MAX as u64 {
            to_compress[i] = zz as u32;
        }
        // If it fails, use last value, remember where it failed
        else {
            to_compress[i] = to_compress[i - 1];
            big_value[big_count as usize] = zz;
            big_offset[big_count as usize] = i as u8;
            big_count += 1;
        }

        last_diff = diff
    }
    // Diff-in-Diff only starts at the 2nd index
    to_compress[0] = to_compress[2];
    to_compress[1] = to_compress[2];

    // Write the length
    buf.extend_from_slice(&timestamps.len().to_be_bytes());

    // Write first and second timestamp
    buf.extend_from_slice(&timestamps[0].to_be_bytes());
    buf.extend_from_slice(&timestamps[1].to_be_bytes());

    // Compress the u32 array
    let sz = bitpack_256_compress(buf, &to_compress);

    // Write the zigzag values too big for u32

    // first write the number of values
    buf.push(big_count);

    // then write every value
    for i in 0..big_count as usize {
        buf.extend_from_slice(&big_value[i].to_be_bytes());
        buf.push(big_offset[i]);
    }
}

/// Decompresses data into buf
/// Returns the number of bytes read from data and number of items decompressed.
/// Panics if buf is not long enough.
pub fn decompress(data: &[u8], buf: &mut Vec<u64>) -> (usize, usize) {
    let mut off = 0;

    // Get len
    let end = off + size_of::<usize>();
    let len = usize::from_be_bytes(<[u8; 8]>::try_from(&data[off..end]).unwrap());
    off = end;

    // Get first and second timestamp
    let end = off + size_of::<u64>();
    buf.push(u64::from_be_bytes(<[u8; 8]>::try_from(&data[off..end]).unwrap()));
    off = end;

    // Get second timestamp
    let end = off + size_of::<u64>();
    buf.push(u64::from_be_bytes(<[u8; 8]>::try_from(&data[off..end]).unwrap()));
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
    let mut last_diff = buf[1] - buf[0];
    for i in 2..len {
        let diff2: i64 = from_zigzag(zigzagged[i]);
        let value = diff2 + buf[i - 1] as i64 + last_diff as i64;
        buf.push(value.try_into().unwrap());
        last_diff = buf[i] - buf[i - 1];
    }

    (off, len)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::*;

    fn compress_decompress(data: &[u64]) {
        let mut buf = vec![0u8; 1024];
        let mut byte_buf = ByteBuffer::new(&mut buf[..]);
        let len = data.len().min(256);
        compress(&data[..len], &mut byte_buf);
        let sz = byte_buf.len();

        let mut res = Vec::new();
        let (b, l) = decompress(&buf[..sz], &mut res);

        assert_eq!(b, sz);
        assert_eq!(l, len);
        assert_eq!(&res[..l], &data[..len]);
    }

    #[test]
    fn test_compress_decompress() {
        for (_, data) in UNIVARIATE_DATA.iter() {
            let mut v = data.iter().map(|x| x.ts).collect::<Vec<u64>>();
            for i in 0..v.len() - 256 {
                compress_decompress(&v[i..i + 256]);
            }
        }
    }

    #[test]
    fn test_compress_decompress_simple() {
        let data = [1, 2, 3, 4, 6, 8, 10];
        let mut buf = vec![0u8; 1024];
        let mut byte_buf = ByteBuffer::new(&mut buf[..]);
        compress(&data[..], &mut byte_buf);
        let sz = byte_buf.len();
        let mut res = Vec::new();
        let (b, l) = decompress(&buf[..sz], &mut res);
        assert_eq!(&res[..l], &data[..]);
    }
}
