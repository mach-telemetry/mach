// Copyright (c) 2023 Franco Solleza, Intel Corporation, Brown University
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

use crate::{byte_buffer::ByteBuffer, compression::CompressDecompress, segment::SegmentArray};
use lzzzz::lz4;
use serde::*;
use std::convert::TryInto;
use std::slice;

#[derive(Copy, Clone, Serialize, Deserialize)]
pub struct LZ4 {}

impl CompressDecompress for LZ4 {
    fn compress(&self, data_len: usize, data: &SegmentArray, buffer: &mut ByteBuffer) {
        compress(data_len, data, buffer);
    }

    fn decompress(&self, data: &[u8], data_len: &mut usize, buffer: &mut SegmentArray) {
        decompress(data, data_len, buffer);
    }
}

fn compress(data_len: usize, data: &SegmentArray, buffer: &mut ByteBuffer) {
    // unnest from chunks
    let data: &[u8] = unsafe {
        let ptr = data.as_ptr() as *const u8;
        slice::from_raw_parts(ptr, 8 * data_len)
    };

    // write raw data length
    buffer.extend_from_slice(&data_len.to_be_bytes());

    // reserve for size
    let size_offset = buffer.len();
    buffer.extend_from_slice(&0usize.to_be_bytes());

    // compress
    let sz = lz4::compress(data, buffer.remaining(), lz4::ACC_LEVEL_DEFAULT).unwrap();
    buffer.set_len(buffer.len() + sz);

    // write size
    buffer.as_mut_slice()[size_offset..size_offset + 8].copy_from_slice(&sz.to_be_bytes());
}

fn decompress(data: &[u8], data_len: &mut usize, buffer: &mut SegmentArray) {
    // read raw data length
    let dl = usize::from_be_bytes(data[..8].try_into().unwrap());
    *data_len = dl;

    // read compressed size
    let compressed_sz = usize::from_be_bytes(data[8..16].try_into().unwrap());
    let src = &data[16..compressed_sz + 16];

    // unnest from chunks based on data length
    let dst: &mut [u8] = unsafe {
        let ptr = buffer.as_ptr() as *mut u8;
        slice::from_raw_parts_mut(ptr, 8 * dl)
    };

    // decompress
    let sz = lz4::decompress(src, dst).unwrap();
    assert_eq!(sz, dl * 8);
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::constants::SEG_SZ;
    use rand::{thread_rng, Rng};

    #[test]
    fn test() {
        let mut rng = thread_rng();
        let data: Vec<[u8; 8]> = (0..SEG_SZ)
            .map(|_| rng.gen::<usize>().to_be_bytes())
            .collect();
        let mut compressed: Vec<u8> = vec![0u8; 1_000_000];
        let mut byte_buffer = ByteBuffer::new(0, &mut compressed[..]);

        compress(
            SEG_SZ,
            data.as_slice().try_into().unwrap(),
            &mut byte_buffer,
        );

        let mut decompressed: Vec<[u8; 8]> = vec![[0u8; 8]; 256];
        let buf: &mut [[u8; 8]] = &mut decompressed[..];
        let mut len: usize = 0;
        decompress(byte_buffer.as_slice(), &mut len, buf.try_into().unwrap());
        assert_eq!(len, SEG_SZ);
        assert_eq!(data, decompressed);
    }
}
