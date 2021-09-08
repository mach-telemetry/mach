use crate::{
    tsdb::{Fl, Dt}
};
use num::NumCast;
use std::convert::TryFrom;
use bitpacking::{BitPacker8x, BitPacker};

pub fn to_zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

pub fn from_zigzag(v: u64) -> i64 {
    (v >> 1) as i64 ^ (-((v & 1) as i64))
}

pub fn multiplier(p: u8) -> i64 {
    10i64.pow(p as u32)
}

pub fn fl_to_int(p: u8, a: Fl) -> Result<i64, ()> {
    let v: f64 = a as f64;
    let sign = v.signum();
    let v = v.abs();

    let m: i64 = multiplier(p);

    let n = v.floor();
    let f = (v - n) * m as f64;

    let nm = n as i64 * m;
    let fm = f as i64;
    let r = (nm + fm) * sign as i64;

    NumCast::from(r).ok_or(())
}

pub fn fl_from_int(p: u8, v: i64) -> Fl {
    let mult = multiplier(p);
    (v as f64 / mult as f64) as Fl
}

pub fn round(p: u8, v: Fl) -> Fl {
    let mult = multiplier(p) as Fl;
    NumCast::from((v * mult).round() / mult).unwrap()
}

#[derive(Copy, Clone)]
pub struct U64Differ {
    prev: i64,
    prev_diff: i64,
    len: usize,
}

impl U64Differ {
    pub fn new() -> Self {
        Self {
            prev: -1,
            prev_diff: 0,
            len: 0,
        }
    }

    pub fn roll(&mut self, ts: u64) -> u64 {
        let ts: i64 = NumCast::from(ts).unwrap();
        let r = if self.len == 0 {
            self.prev = ts;
            NumCast::from(ts).unwrap()
            //to_zigzag(self.prev)
        } else if self.len == 1 {
            self.prev_diff = ts - self.prev;
            self.prev = ts;
            to_zigzag(self.prev_diff)
        } else {
            let diff = ts - self.prev;
            let diff2 = diff - self.prev_diff;
            self.prev_diff = diff;
            self.prev = ts;
            to_zigzag(diff2)
        };
        self.len += 1;
        r
    }

    pub fn unroll(&mut self, v: u64) -> u64 {
        let r = if self.len == 0 {
            self.prev = NumCast::from(v).unwrap();
            NumCast::from(v).unwrap()
        } else if self.len == 1 {
            let v: i64 = from_zigzag(v);
            self.prev_diff = v;
            self.prev += self.prev_diff;
            NumCast::from(self.prev).unwrap()
        } else {
            let v: i64 = from_zigzag(v);
            let value = v + self.prev + self.prev_diff;
            self.prev_diff = value - self.prev;
            self.prev = value;
            NumCast::from(self.prev).unwrap()
        };
        self.len += 1;
        r
    }
}

#[derive(Copy, Clone)]
pub struct I64Differ {
    prev: i64,
    prev_diff: i64,
    len: usize,
}

impl I64Differ {
    pub fn new() -> Self {
        Self {
            prev: -1,
            prev_diff: 0,
            len: 0,
        }
    }

    pub fn roll(&mut self, ts: i64) -> u64 {
        let r = if self.len == 0 {
            self.prev = ts;
            to_zigzag(self.prev)
        } else if self.len == 1 {
            self.prev_diff = ts - self.prev;
            self.prev = ts;
            to_zigzag(self.prev_diff)
        } else {
            let diff = ts - self.prev;
            let diff2 = diff - self.prev_diff;
            self.prev_diff = diff;
            self.prev = ts;
            to_zigzag(diff2)
        };
        self.len += 1;
        r
    }

    pub fn unroll(&mut self, v: u64) -> i64 {
        let r = if self.len == 0 {
            let v: i64 = from_zigzag(v);
            self.prev = v;
            NumCast::from(v).unwrap()
        } else if self.len == 1 {
            let v: i64 = from_zigzag(v);
            self.prev_diff = v;
            self.prev += self.prev_diff;
            NumCast::from(self.prev).unwrap()
        } else {
            let v: i64 = from_zigzag(v);
            let value = v + self.prev + self.prev_diff;
            self.prev_diff = value - self.prev;
            self.prev = value;
            NumCast::from(self.prev).unwrap()
        };
        self.len += 1;
        r
    }
}

///
/// 0..2    -> Size = length of compressed section in bytes
/// 2       -> number of bits required (bitpack.num_bits())
/// ..      -> compressed data of Size bytes
/// returns how many bytes were written into buf
pub fn bitpack_256_compress(buf: &mut [u8], data: &[u32; 256]) -> usize {
    let bitpacker = BitPacker8x::new();

    // Compress the data, reserve first two bytes for size, one byte for the numbits
    let num_bits: u8 = bitpacker.num_bits(&data[..]);
    let size = bitpacker.compress(&data[..], &mut buf[3..], num_bits);

    // store the size of the compressed array
    let size_array: [u8; 2] = <u16 as NumCast>::from(size).unwrap().to_le_bytes();
    buf[..2].copy_from_slice(&size_array[..]);

    // store the num bits
    buf[2] = num_bits;

    size + 3
}

/// See bitpacker_256_compress for layout
/// returns how many bytes were read from data
pub fn bitpack_256_decompress(out: &mut[u32; 256], data: &[u8]) -> usize {
    let bytes: u16 = u16::from_le_bytes(<[u8; 2]>::try_from(&data[..2]).unwrap());
    let num_bits: u8 = data[2];
    let bitpacker = BitPacker8x::new();
    bitpacker.decompress(&data[3..3+bytes as usize], &mut out[..], num_bits);
    bytes as usize + 3
}
