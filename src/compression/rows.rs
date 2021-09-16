use crate::{
    compression::{utils::*, Header},
    segment::{Segment, SegmentLike},
    tsdb::{Dt, Fl},
};
use std::{
    convert::{TryFrom, TryInto},
    mem::size_of,
};

pub fn rows_compression<S: SegmentLike>(
    data: &S,
    final_buf: &mut [u8],
    precision: &[u8],
) -> usize {
    assert!(256 >= data.nvars() * data.len());
    let mut buf = [0u8; 8192];
    let len = data.len();
    let mut scratch: [u32; 256] = [0; 256];
    let mut off = 0;

    // Write the length of the timeseries (< 256)
    buf[off] = <u8>::try_from(len).unwrap();
    off += 1;

    // Write the number of variables and the precision of each
    let nvars = <u8>::try_from(precision.len()).unwrap();
    buf[off] = nvars;
    off += 1;

    let end = off + nvars as usize;
    buf[off..end].copy_from_slice(precision);
    off = end;

    // First timestamp as big endian
    let timestamps = data.timestamps();
    let first = timestamps[0];
    let end = off + size_of::<Dt>();
    buf[off..end].copy_from_slice(&first.to_be_bytes());
    off = end;

    // Compress the timestamps
    let mut big: [u64; 256] = [0; 256];
    let mut big_offset: [u8; 256] = [0; 256];
    let mut big_count: usize = 0;
    let mut diff = U64Differ::new();
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
        buf[off..off+sz].copy_from_slice(&big[j].to_be_bytes()[..]);
        off += sz;
        buf[off] = big_offset[j];
        off += 1;
    }

    big_count = 0; // reset the big arrays
    let mut scratch_offset = 0;

    let mut raw: [Fl; 256] = [0.; 256];
    let mut raw_offset: [u8; 256] = [0; 256];
    let mut raw_var: [u8; 256] = [0; 256];
    let mut raw_count: usize = 0;

    // Then the variables
    for (idx, p) in precision.iter().enumerate() {

        let values = &data.variable(idx);
        let mut diff = I64Differ::new();

        // First value as big endian, handle case where float value can't be rounded because it's
        // too big or small to fit into a int64
        let first_int = match fl_to_int(*p, values[0]) {
            Ok(x) => x,
            Err(_) => {
                //println!("ROW ERR IN FL TO INT FIRST FIELD");
                raw[raw_count] = values[0];
                raw_offset[raw_count] = 0;
                raw_var[raw_count] = idx as u8;
                raw_count += 1;
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
                        //println!("ROW ERR IN FL TO INT");
                        raw[raw_count] = values[j];
                        raw_offset[raw_count] = j as u8;
                        raw_var[raw_count] = idx as u8;
                        raw_count += 1;
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
                Ok(u) => scratch[scratch_offset] = u,
                Err(_) => {
                    scratch[scratch_offset] = 0;
                    big[big_count] = rolled;
                    big_offset[big_count] = scratch_offset as u8;
                    big_count += 1;
                },
            }
            scratch_offset += 1;
        }
    }

    off += bitpack_256_compress(&mut buf[off..], &scratch);

    // Write the items that are too big
    buf[off] = <u8>::try_from(big_count).unwrap();
    off += 1;
    let sz = size_of::<u64>();
    for j in 0..big_count {
        buf[off..off+sz].copy_from_slice(&big[j].to_be_bytes()[..]);
        off += sz;
        buf[off] = big_offset[j];
        off += 1;
    }

    // Write the raw items
    buf[off] = <u8>::try_from(raw_count).unwrap();
    off += 1;
    let sz = size_of::<Fl>();
    for j in 0..raw_count {
        buf[off..off+sz].copy_from_slice(&raw[j].to_be_bytes()[..]);
        off += sz;
        buf[off] = raw_offset[j];
        off += 1;
        buf[off] = raw_var[j];
        off += 1;
    }

    lz4_flex::compress_into(&buf[..off], final_buf, 0).unwrap()
}

pub fn rows_decompression(header: &Header, buf: &mut Segment, compressed: &[u8]) {
    let mut sect = [0u8; 8192];
    let _lz4_sz = lz4_flex::decompress_into(compressed, &mut sect[..], 0).unwrap();
    let mut oft = 0;

    // Read the length
    let len: usize = sect[oft] as usize;
    assert_eq!(len, header.len);
    oft += 1;

    // Read the number of variables and the precision of each
    let nvars: u8 = sect[oft];
    oft += 1;

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
        let value = u64::from_be_bytes(sect[oft..oft+sz].try_into().unwrap());
        oft += sz;
        let offset = sect[oft];
        oft += 1;
        to_unroll[offset as usize] = value;
    }

    let mut differ = U64Differ::new();
    to_unroll[..len].iter().for_each(|x| {
        buf.timestamps.push(first + differ.unroll(*x))
    });

    //for i in 0..len {
    //    buf.ts[i] = first + differ.unroll(to_unroll[i]);
    //}

    // Skip to compressed block and decompress it and move to the u64 buf for unrolling them
    let s = oft + precision.len() * size_of::<Fl>();
    let read = bitpack_256_decompress(&mut d_buf, &sect[s..]);
    for j in 0..256 {
        to_unroll[j] = d_buf[j] as u64;
    }

    // Skip to the too large section, and unpack those too and write them into the unroll buff
    let mut s = s + read;
    let big_count = sect[s];
    s += 1;

    let sz = size_of::<u64>();
    for _ in 0..big_count {
        let value = u64::from_be_bytes(sect[s..s+sz].try_into().unwrap());
        s += sz;
        let offset = sect[s];
        s += 1;
        to_unroll[offset as usize] = value;
    }

    // Variables
    for (idx, p) in precision.iter().enumerate() {
        // First value
        let end = oft + size_of::<Fl>();
        let first = Fl::from_be_bytes(<[u8; size_of::<Fl>()]>::try_from(&sect[oft..end]).unwrap());
        let first_int: i64 = fl_to_int(*p, first).unwrap();
        oft = end;

        let mut differ = I64Differ::new();
        for j in 0..len {
            let unrolled = differ.unroll(to_unroll[idx * len + j]);
            buf.values.push(fl_from_int(*p, first_int + unrolled));
            //buf.values[idx][j] = fl_from_int(*p, first_int + unrolled);
        }
    }

    // Finally, extract the raw values that were written, if any
    let raw_count = sect[s];
    s += 1;

    let sz = size_of::<Fl>();
    for _ in 0..raw_count {
        let value: Fl = Fl::from_be_bytes(sect[s..s+sz].try_into().unwrap());
        s += sz;
        let offset = sect[s] as usize;
        s += 1;
        let var = sect[s] as usize;
        s += 1;
        buf.values[var * len + offset] = value;
    }


    buf.len = len;
}

