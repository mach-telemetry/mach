use crate::constants::SEG_SZ;
use bitpacking::{BitPacker8x, BitPacker};
use std::mem::size_of;

fn to_zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

fn from_zigzag(v: u64) -> i64 {
    (v >> 1) as i64 ^ (-((v & 1) as i64))
}

fn bitpack_256_compress(buffer: &mut Vec<u8>, data: &[u32; 256]) {

    let bitpacker = BitPacker8x::new();
    let num_bits: u8 = bitpacker.num_bits(&data[..]);
    let compressed_sz = BitPacker8x::compressed_block_size(num_bits);
    buffer.extend_from_slice(&compressed_sz.to_be_bytes());
    buffer.push(num_bits);

    let start = buffer.len();
    buffer.resize(start + compressed_sz, 0);
    let size = bitpacker.compress(&data[..], &mut buffer[start..start+compressed_sz], num_bits);
    assert_eq!(size, compressed_sz);
}

fn bitpack_256_decompress(out: &mut [u32; 256], data: &[u8]) {
    let compressed_sz = usize::from_be_bytes(data[..8].try_into().unwrap());
    let num_bits = data[8];
    let bitpacker = BitPacker8x::new();
    bitpacker.decompress(&data[9..compressed_sz + 9], &mut out[..], num_bits);
}

pub fn compress(len: usize, data: &[u64; SEG_SZ], buffer: &mut Vec<u8>) {
    assert!(len <= SEG_SZ);

    // Some scratch pad and locations to remember items too big too store in u32
    let mut to_compress: [u32; 256] = [0; 256];
    let mut big_value: [u64; 256] = [0; 256];
    let mut big_offset: [u8; 256] = [0; 256];
    let mut big_count: u8 = 0;

    let mut last_diff: u64 = data[1] - data[0];
    for i in 2..len {
        let diff = data[i] - data[i - 1];

        // we don't expect this to fail
        let x: Result<i64, _> = diff.try_into();
        if x.is_err() {
            println!(
                "diff: {} timestamps[i]: {}, timestamps[i-1]:: {}",
                diff,
                data[i],
                data[i - 1]
            );
        }
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
    buffer.extend_from_slice(&len.to_be_bytes());

    // Write first and second timestamp
    buffer.extend_from_slice(&data[0].to_be_bytes());
    buffer.extend_from_slice(&data[1].to_be_bytes());

    // Compress the u32 array
    let size_offset = buffer.len();
    buffer.extend_from_slice(&0usize.to_be_bytes());
    let old_size = buffer.len();
    bitpack_256_compress(buffer, &to_compress);
    let new_size = buffer.len();
    buffer[size_offset..size_offset + 8].copy_from_slice(&(new_size - old_size).to_be_bytes());

    // Write the zigzag values too big for u32

    // first write the number of values
    buffer.push(big_count);

    // then write every value
    for i in 0..big_count as usize {
        buffer.extend_from_slice(&big_value[i].to_be_bytes());
        buffer.push(big_offset[i]);
    }
}

pub fn decompress(data: &[u8], data_len: &mut usize, buffer: &mut [u64; SEG_SZ]) {
    let mut off = 0;
    let mut buf_idx = 0;

    // Get len
    let end = off + size_of::<usize>();
    let len = usize::from_be_bytes(data[off..end].try_into().unwrap());
    *data_len = len;
    off = end;

    // Get first and second timestamp
    let end = off + size_of::<u64>();
    buffer[buf_idx] = u64::from_be_bytes(data[off..end].try_into().unwrap());
    buf_idx += 1;
    off = end;

    // Get second timestamp
    let end = off + size_of::<u64>();
    buffer[buf_idx] = u64::from_be_bytes(data[off..end].try_into().unwrap());
    buf_idx += 1;
    off = end;

    // Get size of compressed data
    let end = off + size_of::<usize>();
    let read = usize::from_be_bytes(data[off..end].try_into().unwrap());
    off = end;

    // Read bitpacked data
    let end = off + read;
    let mut unpacked = [0u32; 256];
    bitpack_256_decompress(&mut unpacked, &data[off..end]);
    off = end;

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
    let mut last_diff = buffer[1] - buffer[0];
    for i in 2..len {
        let diff2: i64 = from_zigzag(zigzagged[i]);
        let value = diff2 + buffer[i - 1] as i64 + last_diff as i64;
        buffer[buf_idx] = value.try_into().unwrap();
        buf_idx += 1;
        last_diff = buffer[i] - buffer[i - 1];
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
        let mut integers: Vec<u64> = Vec::new();
        let mut sum: u64 = rng.gen();
        for x in increments {
            integers.push(sum);
            sum += x;
        }

        assert_eq!(integers.len(), 256);
        let mut compressed_bytes = Vec::new();
        let to_compress: &[u64; SEG_SZ] = integers.as_slice().try_into().unwrap();
        compress(256, to_compress, &mut compressed_bytes);

        let mut len = 0;
        let mut decompressed: Vec<u64> = vec![0u64; 256];
        let decompress_buffer: &mut[u64; 256] = decompressed.as_mut_slice().try_into().unwrap();
        decompress(&compressed_bytes, &mut len, decompress_buffer);

        assert_eq!(len, 256);
        assert_eq!(integers.as_slice(), &decompress_buffer[..]);
    }
}

