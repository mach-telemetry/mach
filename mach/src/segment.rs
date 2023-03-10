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
    constants::{HEAP_SZ, SEG_SZ},
    field_type::FieldType,
    sample::SampleType,
};
use serde::{Deserialize, Serialize};

pub type SegmentArray = [[u8; 8]; SEG_SZ];

pub fn bytes_to_columns(bytes: &[u8]) -> &[SegmentArray] {
    let len = bytes.len();
    let ptr = bytes.as_ptr() as *const SegmentArray;

    assert_eq!(len % (8 * 256), 0);

    unsafe { std::slice::from_raw_parts(ptr, len / (8 * 256)) }
}

pub fn bytes_to_columns_mut(bytes: &mut [u8]) -> &mut [SegmentArray] {
    let len = bytes.len();
    let ptr = bytes.as_ptr() as *mut SegmentArray;

    assert_eq!(len % (8 * 256), 0);

    unsafe { std::slice::from_raw_parts_mut(ptr, len / (8 * 256)) }
}

#[derive(Copy, Clone)]
pub struct SegmentRef<'a> {
    pub len: usize,
    pub heap_len: usize,
    pub timestamps: &'a [u64; SEG_SZ],
    pub heap: &'a [u8],
    pub data: &'a [u8],
    pub types: &'a [FieldType],
}

impl<'a> SegmentRef<'a> {
    pub fn to_segment(&self) -> Segment {
        let len = self.len;
        let heap_len = self.heap_len;
        let timestamps: Box<[u64]> = self.timestamps[..].into();
        let heap: Box<[u8]> = self.heap[..].into();
        let data: Vec<u8> = self.data.into();
        let types: Vec<FieldType> = self.types.into();
        Segment {
            len,
            heap_len,
            timestamps,
            heap,
            data,
            types,
        }
    }

    pub fn columns(&self) -> &[SegmentArray] {
        bytes_to_columns(self.data)
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Segment {
    pub len: usize,
    pub heap_len: usize,
    pub timestamps: Box<[u64]>,
    pub heap: Box<[u8]>,
    pub data: Vec<u8>,
    pub types: Vec<FieldType>,
}

impl Segment {
    pub fn new_empty() -> Self {
        Segment {
            len: 0,
            heap_len: 0,
            timestamps: box [0; SEG_SZ],
            heap: box [0; HEAP_SZ * 2],
            data: Vec::new(),
            types: Vec::new(),
        }
    }

    pub fn columns(&self) -> &[SegmentArray] {
        bytes_to_columns(self.data.as_slice())
    }

    pub fn columns_mut(&mut self) -> &mut [SegmentArray] {
        bytes_to_columns_mut(self.data.as_mut_slice())
    }

    pub fn set_fields(&mut self, types: &mut [FieldType]) {
        self.data.clear();
        self.data.resize(types.len() * 8 * 256, 0);
        self.types.clear();
        self.types.extend_from_slice(types);
    }

    pub fn clear_fields(&mut self) {
        self.data.clear();
        self.types.clear();
    }

    pub fn add_field(&mut self, field: FieldType) {
        let l = self.data.len();
        self.data.resize(l + 8 * 256, 0);
        self.types.push(field);
    }

    pub fn field_idx(&self, field: usize, idx: usize) -> SampleType {
        let cols = self.columns();
        let value = cols[field][idx];
        let field_type = self.types[field];
        match field_type {
            FieldType::Bytes => {
                SampleType::from_field_item(field_type, value, Some(&self.heap[..]))
            }
            _ => SampleType::from_field_item(field_type, value, None),
        }
    }

    pub fn row(&self, idx: usize, buffer: &mut Vec<SampleType>) {
        for i in 0..self.types.len() {
            buffer.push(self.field_idx(i, idx));
        }
    }
}

pub struct SegmentIterator<'a> {
    segment: &'a Segment,
    idx: usize,
    sample: Vec<SampleType>,
    nvars: usize,
}

impl<'a> SegmentIterator<'a> {
    pub fn new(segment: &'a Segment) -> Self {
        SegmentIterator {
            segment,
            idx: segment.len,
            sample: Vec::new(),
            nvars: segment.types.len(),
        }
    }

    pub fn next_sample(&mut self) -> Option<(u64, &[SampleType])> {
        if self.idx == 0 {
            None
        } else {
            self.sample.clear();
            self.idx -= 1;
            for i in 0..self.nvars {
                self.sample.push(self.segment.field_idx(i, self.idx));
            }
            Some((self.segment.timestamps[self.idx], self.sample.as_slice()))
        }
    }
}
