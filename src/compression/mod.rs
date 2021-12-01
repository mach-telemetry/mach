mod timestamps;
mod fixed;
mod xor;
mod utils;

use crate::segment::FullSegment;
use lzzzz::lz4;
use std::convert::TryInto;

const MAGIC: &str = "202107280428";

pub struct DecompressBuffer {
    ts: Vec<u64>,
    values: Vec<[u8; 8]>,
    len: usize,
    nvars: usize,
}

impl DecompressBuffer {
    fn clear(&mut self) {
        self.ts.clear();
        self.values.clear();
        self.len = 0;
        self.nvars = 0;
    }

    fn new() -> Self {
        Self {
            ts: Vec::new(),
            values: Vec::new(),
            len: 0,
            nvars: 0,
        }
    }

    fn timestamps(&self) -> &[u64] {
        &self.ts[..self.len]
    }

    fn values(&self, var: usize) -> &[[u8; 8]] {
        let start = var * self.len;
        &self.values[start..start + self.len]
    }
}


// Compression format
// Magic: [0..12]
// Compression type: [12..13]
// Size in bytes: [13..17]
// Data: [17..17 + size in bytes]
pub enum Compression {
    LZ4(i32)
}

impl Compression {
    fn header(&self, segmen: &FullSegment, buf: &mut Vec<u8>) {
    }

    pub fn compress(&self, segment: &FullSegment, buf: &mut Vec<u8>) {
        match self {
            Compression::LZ4(acc) => lz4_compress(segment, buf, *acc),
        }
    }

    pub fn decompress(data: &mut [u8], buf: &mut DecompressBuffer) {
        buf.clear();
    }
}

fn lz4_compress(segment: &FullSegment, buf: &mut Vec<u8>, acc: i32) {
    let mut len: u64 = segment.len as u64;
    let mut nvars: u64 = segment.nvars as u64;

    let mut bytes = Vec::new();
    for ts in segment.timestamps().iter() {
        bytes.extend_from_slice(&ts.to_be_bytes()[..]);
    }

    for var in 0..nvars {
        for v in segment.values(var as usize).iter() {
            bytes.extend_from_slice(&v[..]);
        }
    }
    let raw_sz = bytes.len() as u64;

    // Number of variables
    buf.extend_from_slice(&nvars.to_be_bytes()[..]);

    // Number of samples
    buf.extend_from_slice(&len.to_be_bytes()[..]);

    // Uncompressed size
    buf.extend_from_slice(&raw_sz.to_be_bytes()[..]);

    // write a place holder of compressed size
    let csz_off = buf.len();
    buf.extend_from_slice(&0u64.to_be_bytes()[..]);         // compressed sz placeholder

    // Compress the raw data and record the compressed size
    let oldlen = buf.len();
    lz4::compress_to_vec(&bytes[..], buf, acc).unwrap();
    let newlen = buf.len();
    let csz = (newlen - oldlen) as u64;

    // Write the compressed size
    buf[csz_off..csz_off + 8].copy_from_slice(&csz.to_be_bytes()[..]);
}

fn lz4_decompress(data: &[u8], buf: &mut DecompressBuffer) {
    let mut off = 0;

    let nvars = u64::from_be_bytes(data[off..off+8].try_into().unwrap());
    off += 8;

    let len = u64::from_be_bytes(data[off..off+8].try_into().unwrap());
    off += 8;

    let raw_sz = u64::from_be_bytes(data[off..off+8].try_into().unwrap());
    off += 8;

    let cmp_sz = u64::from_be_bytes(data[off..off+8].try_into().unwrap());
    off += 8;

    let mut bytes = vec![0u8; raw_sz as usize].into_boxed_slice();
    lz4::decompress(&data[off..off+cmp_sz as usize], &mut bytes[..]).unwrap();

    let mut off = 0;
    for i in 0..len {
        buf.ts.push(u64::from_be_bytes(bytes[off..off + 8].try_into().unwrap()));
        off += 8;
    }

    for var in 0..nvars {
        for i in 0..len {
            buf.values.push(bytes[off..off + 8].try_into().unwrap());
            off += 8;
        }
    }

    buf.nvars = nvars as usize;
    buf.len = len as usize;
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::*;

    #[test]
    fn test_lz4() {

        let data = &MULTIVARIATE_DATA[0].1;
        let nvars = data[0].values.len();

        let mut timestamps = [0; 256];
        let mut v = Vec::new();
        for _ in 0..nvars {
            v.push([[0u8; 8]; 256]);
        }

        for (idx, sample) in data[0..256].iter().enumerate() {
            timestamps[idx] = sample.ts;
            for (var, val) in sample.values.iter().enumerate() {
                v[var][idx] = val.to_be_bytes();
            }
        }

        let segment = FullSegment {
            len: 256,
            nvars,
            ts: &timestamps,
            data: v.as_slice(),
        };

        let mut compressed = Vec::new();
        lz4_compress(&segment, &mut compressed, 1);

        let mut buf = DecompressBuffer::new();
        lz4_decompress(&compressed[..], &mut buf);

        assert_eq!(&buf.ts[..], &timestamps[..]);
        for i in 0..nvars {
            assert_eq!(buf.values(i), segment.values(i));
        }
    }
}
