use crate::utils::byte_buffer::ByteBuffer;
use bitpacking::{BitPacker, BitPacker8x};
use num::NumCast;
use std::convert::TryFrom;

pub fn multiplier(p: u8) -> i64 {
    10i64.pow(p as u32 + 1)
}

pub fn to_zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

pub fn from_zigzag(v: u64) -> i64 {
    (v >> 1) as i64 ^ (-((v & 1) as i64))
}

// p is the number of decimals to round to
//pub fn float_to_parts(v: f64, p: u8) -> (i64, u64) {
//    let integral = v.abs().trunc();
//    let part = ((v - integral) * 10f64.powi(p as i32 + 1)).trunc();
//    (integral as i64, part as u64)
//}
//
//pub fn float_from_parts(i: i64, d: u64, p: u8) -> f64 {
//    let i = i as f64;
//    let d = d as f64;
//    i + (d / 10f64.powi(p as i32 + 1))
//}

#[allow(clippy::many_single_char_names)]
pub fn float_to_int(p: u8, a: f64) -> Result<i64, ()> {
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

pub fn float_from_int(p: u8, v: i64) -> f64 {
    let mult = multiplier(p);
    (v as f64 / mult as f64) as f64
}

//#[derive(Copy, Clone)]
//pub struct U64Differ {
//    prev: i64,
//    prev_diff: i64,
//    len: usize,
//}
//
//impl U64Differ {
//    pub fn new() -> Self {
//        Self {
//            prev: -1,
//            prev_diff: 0,
//            len: 0,
//        }
//    }
//
//    pub fn roll(&mut self, ts: u64) -> u64 {
//        let ts: i64 = NumCast::from(ts).unwrap();
//        let r = if self.len == 0 {
//            self.prev = ts;
//            NumCast::from(ts).unwrap()
//            //to_zigzag(self.prev)
//        } else if self.len == 1 {
//            self.prev_diff = ts - self.prev;
//            self.prev = ts;
//            to_zigzag(self.prev_diff)
//        } else {
//            let diff = ts - self.prev;
//            let diff2 = diff - self.prev_diff;
//            self.prev_diff = diff;
//            self.prev = ts;
//            to_zigzag(diff2)
//        };
//        self.len += 1;
//        r
//    }
//
//    pub fn unroll(&mut self, v: u64) -> u64 {
//        let r = if self.len == 0 {
//            self.prev = NumCast::from(v).unwrap();
//            NumCast::from(v).unwrap()
//        } else if self.len == 1 {
//            let v: i64 = from_zigzag(v);
//            self.prev_diff = v;
//            self.prev += self.prev_diff;
//            NumCast::from(self.prev).unwrap()
//        } else {
//            let v: i64 = from_zigzag(v);
//            let value = v + self.prev + self.prev_diff;
//            self.prev_diff = value - self.prev;
//            self.prev = value;
//            NumCast::from(self.prev).unwrap()
//        };
//        self.len += 1;
//        r
//    }
//}
//
//#[derive(Copy, Clone)]
//pub struct I64Differ {
//    prev: i64,
//    prev_diff: i64,
//    len: usize,
//}
//
//impl I64Differ {
//    pub fn new() -> Self {
//        Self {
//            prev: -1,
//            prev_diff: 0,
//            len: 0,
//        }
//    }
//
//    pub fn roll(&mut self, ts: i64) -> u64 {
//        let r = if self.len == 0 {
//            self.prev = ts;
//            to_zigzag(self.prev)
//        } else if self.len == 1 {
//            self.prev_diff = ts - self.prev;
//            self.prev = ts;
//            to_zigzag(self.prev_diff)
//        } else {
//            let diff = ts - self.prev;
//            let diff2 = diff - self.prev_diff;
//            self.prev_diff = diff;
//            self.prev = ts;
//            to_zigzag(diff2)
//        };
//        self.len += 1;
//        r
//    }
//
//    pub fn unroll(&mut self, v: u64) -> i64 {
//        let v: i64 = from_zigzag(v);
//        let r = if self.len == 0 {
//            self.prev = v;
//            NumCast::from(v).unwrap()
//        } else if self.len == 1 {
//            self.prev_diff = v;
//            self.prev += self.prev_diff;
//            NumCast::from(self.prev).unwrap()
//        } else {
//            let value = v + self.prev + self.prev_diff;
//            self.prev_diff = value - self.prev;
//            self.prev = value;
//            NumCast::from(self.prev).unwrap()
//        };
//        self.len += 1;
//        r
//    }
//}

///
/// 0..2    -> Size = length of compressed section in bytes
/// 2       -> number of bits required (bitpack.num_bits())
/// ..      -> compressed data of Size bytes
/// returns how many bytes were written into buf
pub fn bitpack_256_compress(v: &mut ByteBuffer, data: &[u32; 256]) -> usize {
    let bitpacker = BitPacker8x::new();

    //let len = v.len();
    let header = 3;
    //let maxsz = 1024;

    let buf = v.unused();

    // Compress the data, reserve first two bytes for size, one byte for the numbits
    let num_bits: u8 = bitpacker.num_bits(&data[..]);
    let size = bitpacker.compress(&data[..], &mut buf[header..], num_bits);
    // store the size of the compressed array
    let size_array: [u8; 2] = <u16 as NumCast>::from(size).unwrap().to_le_bytes();
    buf[..2].copy_from_slice(&size_array[..]);

    // store the num bits
    buf[2] = num_bits;
    //drop(buf);

    v.add_len(header + size);

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
    use crate::test_utils::*;
    use fixed::prelude::*;
    use fixed::traits::{FromFixed, LossyFrom};
    use fixed::{types::extra::*, FixedI16, FixedI64};
    use rand::prelude::*;

    //#[test]
    //fn test_float_parts() {
    //    for (_, data) in UNIVARIATE_DATA.iter() {
    //        let mut values = data.iter().map(|x| x.values[0]).collect::<Vec<f64>>();
    //        for v in values {
    //            let (integral, decimal) = float_to_parts(v, 3);
    //            let recons = float_from_parts(integral, decimal, 3);
    //            if (v - recons).abs() >= 0.001 {
    //                println!("{} {} {}", v, recons, (v - recons).abs());
    //                assert!(false);
    //            }
    //        }
    //    }
    //}

    #[test]
    fn test_float_fixed() {
        let v: f64 = -123.456;
        println!("{:#018b}", v.to_bits());
        let v: f64 = -123.458;
        println!("{:#018b}", v.to_bits());

        let conv1 = FixedI64::<U10>::from_num(-123.456);
        println!("{:#018b}", conv1.to_bits());
        let conv2 = FixedI64::<U10>::from_num(-123.458);
        println!("{:#018b}", conv2.to_bits());
        println!("{:#018b}", conv1.to_bits() ^ conv2.to_bits());
        println!("{:#018b}", to_zigzag(conv2.to_bits() - conv1.to_bits()));

        let conv1 = FixedI64::<U10>::from_num(-123456789.456);
        println!("{:#018b}", conv1.to_bits());
        let conv2 = FixedI64::<U10>::from_num(-123456791.123);
        println!("{:#018b}", conv2.to_bits());
        println!("{:#018b}", conv1.to_bits() ^ conv2.to_bits());
        println!("{:#018b}", to_zigzag(conv2.to_bits() - conv1.to_bits()));

        //let data = &UNIVARIATE_DATA[2].1;
        //let mut values = data.iter().map(|x| x.values[0]).collect::<Vec<f64>>();
        //for i in 1..values.len() {
        //    let old = FixedI64::<U10>::from_num(values[i-1]);
        //    let cur = FixedI64::<U10>::from_num(values[i]);
        //    let diff = cur - old;
        //    println!("{} {} {} {} {} {}", values[i], cur, values[i-1], old, diff, diff.to_bits());
        //}
    }

    //#[test]
    //fn u64_differences_roll() {
    //    let mut state = U64Differ::new();
    //    assert_eq!(state.roll(11), 11); // first is not zigged
    //    assert_eq!(state.roll(15), to_zigzag(4));
    //    assert_eq!(state.roll(16), to_zigzag(-3));
    //    assert_eq!(state.roll(20), to_zigzag(3));
    //    assert_eq!(state.roll(21), to_zigzag(-3));
    //}

    //#[test]
    //fn u64_differences_unroll() {
    //    let data = &[11, 15, 16, 20, 21];
    //    let mut state = U64Differ::new();
    //    let rolled: Vec<u64> = data.iter().map(|x| state.roll(*x)).collect();

    //    let mut state = U64Differ::new();
    //    let unrolled: Vec<u64> = rolled.iter().map(|x| state.unroll(*x)).collect();
    //    assert_eq!(&data[..], &unrolled);
    //}

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
        let mut buf = vec![0u8; 4096];
        let mut byte_buf = ByteBuffer::new(&mut buf[..]);
        let data = &mut [0u32; 256];
        let res = &mut [0u32; 256];
        rng.fill(&mut data[..]);
        rng.fill(&mut res[..]);
        let sz = bitpack_256_compress(&mut byte_buf, data);
        bitpack_256_decompress(res, &buf[..sz]);
        assert_eq!(res, data);
    }
}
