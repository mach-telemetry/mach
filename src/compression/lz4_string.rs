use crate::segment::FullSegment;
use crate::utils::byte_buffer::ByteBuffer;
use lzzzz::lz4;
use std::convert::TryInto;
use crate::compression::Error;

pub fn string_lz4_compress(segment: &[[u8; 8]], buf: &mut ByteBuffer) {
    //let mut len: u64 = segment.len as u64;
    let nvars: u64 = segment.nvars as u64;

    let mut bytes = Vec::new();
    for ts in segment.timestamps().iter() {
        bytes.extend_from_slice(&ts.to_be_bytes()[..]);
    }

    for var in 0..nvars {
        for v in segment.variable(var as usize).iter() {
            bytes.extend_from_slice(&v[..]);
        }
    }
    let raw_sz = bytes.len() as u64;

    // Uncompressed size
    buf.extend_from_slice(&raw_sz.to_be_bytes()[..]);

    // write a place holder of compressed size
    let csz_off = buf.len();
    buf.extend_from_slice(&0u64.to_be_bytes()[..]); // compressed sz placeholder

    // Compress the raw data and record the compressed size
    let csz = lz4::compress(&bytes[..], buf.unused(), acc).unwrap();
    buf.add_len(csz);

    //println!("LZ4 Compress: len {} nvars {} csz {} buflen {}", len, nvars, csz, buf.len());

    // Write the compressed size
    buf.as_mut_slice()[csz_off..csz_off + 8].copy_from_slice(&csz.to_be_bytes()[..]);
}

pub fn string_lz4_decompress(data: &[u8], buf: &mut Vec<[u8; 8]>, frac: usize) -> (usize, usize) {
    let mut off = 0;

    let raw_sz = u64::from_be_bytes(data[off..off + 8].try_into().unwrap());
    off += 8;

    let cmp_sz = u64::from_be_bytes(data[off..off + 8].try_into().unwrap());
    off += 8;

    let mut bytes = vec![0u8; raw_sz as usize].into_boxed_slice();
    lz4::decompress(&data[off..off + cmp_sz as usize], &mut bytes[..]).unwrap();

    let mut off = 0;
    for _ in 0..header.len {
        buf.ts
            .push(u64::from_be_bytes(bytes[off..off + 8].try_into().unwrap()));
        off += 8;
    }

    for var in 0..header.nvars {
        for _ in 0..header.len {
            buf.values[var].push(bytes[off..off + 8].try_into().unwrap());
            off += 8;
        }
    }

    Ok(off)
}


