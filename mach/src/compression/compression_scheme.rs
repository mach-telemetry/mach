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

use crate::{
    byte_buffer::ByteBuffer,
    compression::{delta_of_delta::DeltaOfDelta, lz4::LZ4, CompressDecompress},
    segment::SegmentArray,
};
use serde::*;

#[derive(Clone, Serialize, Deserialize)]
pub enum CompressionScheme {
    LZ4(LZ4),
    DeltaOfDelta(DeltaOfDelta),
}

impl CompressionScheme {
    pub fn compress(&self, len: usize, data: &SegmentArray, buffer: &mut ByteBuffer) {
        match self {
            Self::LZ4(l) => l.compress(len, data, buffer),
            Self::DeltaOfDelta(l) => l.compress(len, data, buffer),
        };
    }

    pub fn decompress(&self, data: &[u8], len: &mut usize, buffer: &mut SegmentArray) {
        match self {
            Self::LZ4(l) => l.decompress(data, len, buffer),
            Self::DeltaOfDelta(l) => l.decompress(data, len, buffer),
        };
    }

    pub fn lz4() -> Self {
        Self::LZ4(LZ4 {})
    }

    pub fn delta_of_delta() -> Self {
        Self::DeltaOfDelta(DeltaOfDelta {})
    }
}
