use crate::{
    compression::{utils::*, Header},
    segment::{Segment, SegmentLike},
    tsdb::{Dt, Fl},
};
use std::{
    convert::{TryFrom, TryInto},
    mem::size_of,
};

//pub type CompressFn<S> = fn(segment: S, opts: SeriesOptions, buf: &mut [u8]) -> usize;
pub fn simple_compression<S: SegmentLike>(
    data: &S,
    final_buf: &mut [u8],
    precision: &[u8],
) -> usize {
    let mut buf = [0u8; 8192];
    let len = data.len();
    let mut scratch: [u32; 256] = [0; 256];
    let mut off = 0;

    // Write the number of variables
    let nvars = <u8>::try_from(precision.len()).unwrap();
    buf[off] = nvars;
    off += 1;

    // Precisions
    let end = off + nvars as usize;
    buf[off..end].copy_from_slice(precision);
    off = end;

    let timestamps = data.timestamps();
    // First timestamp as big endian
    let first = timestamps[0];
    let end = off + size_of::<Dt>();
    buf[off..end].copy_from_slice(&first.to_be_bytes());
    off = end;

    // Compress the timestamps
    let mut big: [u64; 256] = [0; 256];
    let mut big_offset: [u8; 256] = [0; 256];
    let mut big_count: usize = 0;

    let mut diff = U64Differ::new();

    #[allow(clippy::needless_range_loop)]
    for i in 0..len {
        let d_first = timestamps[i] - first;
        let v = diff.roll(d_first);
        match <u32>::try_from(v) {
            Ok(f) => scratch[i] = f,
            Err(_) => {
                scratch[i] = 0;
                big[big_count] = v;
                big_offset[big_count] = i as u8;
                big_count += 1;
            }
        }
    }
    off += bitpack_256_compress(&mut buf[off..], &scratch);
    // Write the items that are too big
    buf[off] = <u8>::try_from(big_count).unwrap();
    off += 1;
    let sz = size_of::<u64>();
    for j in 0..big_count {
        buf[off..off + sz].copy_from_slice(&big[j].to_be_bytes()[..]);
        off += sz;
        buf[off] = big_offset[j];
        off += 1;
    }

    let mut raw: [Fl; 256] = [0.; 256];
    let mut raw_offset: [u8; 256] = [0; 256];
    let mut _raw_count: usize = 0;

    // Then the variables
    for (idx, p) in precision.iter().enumerate() {
        big_count = 0; // reset the big arrays
        _raw_count = 0; // raw -> there are items we can't encode

        let values = &data.variable(idx);
        let mut diff = I64Differ::new();

        // First value as big endian, handle case where float value can't be rounded because it's
        // too big or small to fit into a int64
        let first_int = match fl_to_int(*p, values[0]) {
            Ok(x) => x,
            Err(_) => {
                //println!("SIMPLE ERR IN FL TO INT FIRST FIELD");
                raw[0] = values[0];
                raw_offset[0] = 0;
                _raw_count += 1;
                0
            }
        };
        let end = off + size_of::<Fl>();
        buf[off..end].copy_from_slice(&values[0].to_be_bytes());
        off = end;

        let mut last_int = first_int;
        for j in 0..len {
            // Handle cases where float can't rounded into i64, in these cases, use the last int
            let int = match fl_to_int(*p, values[j]) {
                Ok(x) => x,
                Err(_) => {
                    if j > 0 {
                        //println!("SIMPLE ERR IN FL TO INT");
                        raw[_raw_count] = values[j];
                        raw_offset[_raw_count] = j as u8;
                        _raw_count += 1;
                        last_int
                    } else {
                        first_int
                    }
                }
            };
            last_int = int;
            let x = int - first_int;
            let rolled = diff.roll(x);
            match <u32>::try_from(rolled) {
                Ok(u) => scratch[j] = u,
                Err(_) => {
                    scratch[j] = 0;
                    big[big_count] = rolled;
                    big_offset[big_count] = j as u8;
                    big_count += 1;
                }
            }
        }
        off += bitpack_256_compress(&mut buf[off..], &scratch);

        // Write the items that are too big
        buf[off] = <u8>::try_from(big_count).unwrap();
        off += 1;
        let sz = size_of::<u64>();
        for j in 0..big_count {
            buf[off..off + sz].copy_from_slice(&big[j].to_be_bytes()[..]);
            off += sz;
            buf[off] = big_offset[j];
            off += 1;
        }

        // Write the raw items
        buf[off] = <u8>::try_from(_raw_count).unwrap();
        off += 1;
        let sz = size_of::<Fl>();
        for j in 0.._raw_count {
            buf[off..off + sz].copy_from_slice(&raw[j].to_be_bytes()[..]);
            off += sz;
            buf[off] = raw_offset[j];
            off += 1;
        }
    }

    lz4_flex::compress_into(&buf[..off], final_buf, 0).unwrap()
}

pub fn simple_decompression(header: &Header, buf: &mut Segment, compressed: &[u8]) {
    let mut sect = [0u8; 8192];
    let _lz4_sz = lz4_flex::decompress_into(compressed, &mut sect[..], 0).unwrap();

    let len = header.len;
    let mut oft = 0;

    // Read the number of variables
    let nvars: u8 = sect[oft];
    oft += 1;

    // Precision
    let end = oft + nvars as usize;
    let precision: &[u8] = &sect[oft..end];
    oft = end;

    // First timetsamp
    let end = oft + size_of::<Dt>();
    let first = Dt::from_be_bytes(<[u8; 8]>::try_from(&sect[oft..end]).unwrap());
    oft = end;

    //let mut sink = Section256Sink::<u64>::new();
    let mut d_buf = [0u32; 256];
    let mut to_unroll = [0u64; 256];
    //let mut values_rolled: Vec<u32> = Vec::new();

    // Timeseries
    let read = bitpack_256_decompress(&mut d_buf, &sect[oft..]);
    oft += read;
    for j in 0..256 {
        to_unroll[j] = d_buf[j] as u64;
    }

    let big_count = sect[oft];
    oft += 1;

    let sz = size_of::<u64>();
    for _ in 0..big_count {
        let value = u64::from_be_bytes(sect[oft..oft + sz].try_into().unwrap());
        oft += sz;
        let offset = sect[oft];
        oft += 1;
        to_unroll[offset as usize] = value;
    }

    let mut differ = U64Differ::new();

    for v in &to_unroll[..len] {
        buf.timestamps.push(first + differ.unroll(*v));
    }
    //for i in 0..len {
    //    buf.timestamps.push(first + differ.unroll(to_unroll[i]));
    //}

    // Variables
    for (idx, p) in precision.iter().enumerate() {
        // First value
        let end = oft + size_of::<Fl>();
        let first = Fl::from_be_bytes(<[u8; size_of::<Fl>()]>::try_from(&sect[oft..end]).unwrap());
        let first_int: i64 = fl_to_int(*p, first).unwrap();
        oft = end;

        let read = bitpack_256_decompress(&mut d_buf, &sect[oft..]);
        oft += read;

        let mut to_unroll = [0u64; 256];
        for j in 0..256 {
            to_unroll[j] = d_buf[j] as u64;
        }

        // Get the diffs that were too big
        let big_count = sect[oft];
        oft += 1;

        let sz = size_of::<u64>();
        for _ in 0..big_count {
            let value = u64::from_be_bytes(sect[oft..oft + sz].try_into().unwrap());
            oft += sz;
            let offset = sect[oft];
            oft += 1;
            to_unroll[offset as usize] = value;
        }

        let mut differ = I64Differ::new();

        for v in &to_unroll[..len] {
            let unrolled = differ.unroll(*v);
            buf.values.push(fl_from_int(*p, first_int + unrolled));
        }

        //for j in 0..len {
        //    let unrolled = differ.unroll(to_unroll[j]);
        //    buf.values.push(fl_from_int(*p, first_int + unrolled));
        //}

        // Get the raw values that couldn't be cast to an i64
        let raw_count = sect[oft];
        oft += 1;

        let sz = size_of::<Fl>();
        for _ in 0..raw_count {
            let value = Fl::from_be_bytes(sect[oft..oft + sz].try_into().unwrap());
            oft += sz;
            let offset = sect[oft];
            oft += 1;
            buf.values[idx * len + offset as usize] = value;
        }
    }

    buf.len = len;
}
