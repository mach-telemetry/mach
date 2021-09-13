use crate::tsdb::Fl;
use bitpacking::{BitPacker, BitPacker8x};
use num::NumCast;
use std::convert::TryFrom;

pub fn to_zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

pub fn from_zigzag(v: u64) -> i64 {
    (v >> 1) as i64 ^ (-((v & 1) as i64))
}

pub fn multiplier(p: u8) -> i64 {
    10i64.pow(p as u32)
}

#[allow(clippy::many_single_char_names)]
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

//pub fn round(p: u8, v: Fl) -> Fl {
//    let mult = multiplier(p) as Fl;
//    NumCast::from((v * mult).round() / mult).unwrap()
//}

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
        let v: i64 = from_zigzag(v);
        let r = if self.len == 0 {
            self.prev = v;
            NumCast::from(v).unwrap()
        } else if self.len == 1 {
            self.prev_diff = v;
            self.prev += self.prev_diff;
            NumCast::from(self.prev).unwrap()
        } else {
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
pub fn bitpack_256_decompress(out: &mut [u32; 256], data: &[u8]) -> usize {
    let bytes: u16 = u16::from_le_bytes(<[u8; 2]>::try_from(&data[..2]).unwrap());
    let num_bits: u8 = data[2];
    let bitpacker = BitPacker8x::new();
    bitpacker.decompress(&data[3..3 + bytes as usize], &mut out[..], num_bits);
    bytes as usize + 3
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::prelude::*;

    #[test]
    fn u64_differences_roll() {
        let mut state = U64Differ::new();
        assert_eq!(state.roll(11), 11); // first is not zigged
        assert_eq!(state.roll(15), to_zigzag(4));
        assert_eq!(state.roll(16), to_zigzag(-3));
        assert_eq!(state.roll(20), to_zigzag(3));
        assert_eq!(state.roll(21), to_zigzag(-3));
    }

    #[test]
    fn u64_differences_unroll() {
        let data = &[11, 15, 16, 20, 21];
        let mut state = U64Differ::new();
        let rolled: Vec<u64> = data.iter().map(|x| state.roll(*x)).collect();

        let mut state = U64Differ::new();
        let unrolled: Vec<u64> = rolled.iter().map(|x| state.unroll(*x)).collect();
        assert_eq!(&data[..], &unrolled);
    }

    //#[test]
    //fn fl_differences_roll() {
    //    let mut state = FlDiffer::new(2);
    //    assert_eq!(state.roll(1.2345), to_zigzag(123));
    //    assert_eq!(state.roll(2.3456), to_zigzag(235 - 123));
    //    assert_eq!(state.roll(3.4567), to_zigzag((346 - 235) - (235 - 123)));
    //    assert_eq!(state.roll(4.5678), to_zigzag((457 - 346) - (346 - 235)));
    //    assert_eq!(state.roll(2.4517), to_zigzag((245 - 457) - (457 - 346)));
    //    assert_eq!(state.roll(1.6719), to_zigzag((167 - 245) - (245 - 457)));
    //    assert_eq!(state.roll(-0.1234), to_zigzag((-12 - 167) - (167 - 245)));
    //    assert_eq!(
    //        state.roll(-10.7823),
    //        to_zigzag((-1078 - (-12)) - ((-12) - 167))
    //    );
    //    assert_eq!(
    //        state.roll(-2.3416),
    //        to_zigzag((-234 - (-1078)) - (-1078 - (-12)))
    //    );
    //}

    //#[test]
    //fn fl_differences_unroll() {
    //    let mut state = FlDiffer::new(2);
    //    let data: &[Fl] = &[
    //        1.2345, 2.3456, 3.4567, 4.567, 2.451, 1.671, -0.123, -10.782, -2.341,
    //    ];
    //    let exp: Vec<Fl> = data.iter().map(|x| (x * 100.).round() / 100.).collect();
    //    let rolled: Vec<u64> = data.iter().map(|x| state.roll(*x)).collect();

    //    let mut state = FlDiffer::new(2);
    //    let unrolled: Vec<Fl> = rolled.iter().map(|x| state.unroll(*x)).collect();
    //    assert_eq!(exp, unrolled);
    //}

    #[test]
    fn bitpack_compress_decompress() {
        let mut rng = thread_rng();
        let buf = &mut [0u8; 8192];
        let data = &mut [0u32; 256];
        let res = &mut [0u32; 256];
        rng.fill(&mut data[..]);
        rng.fill(&mut buf[..]);
        rng.fill(&mut res[..]);
        let _sz = bitpack_256_compress(buf, data);
        bitpack_256_decompress(res, buf);
        assert_eq!(res, data);
    }
}
