#![feature(box_syntax)]
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

#![feature(slice_as_chunks)]
#![feature(maybe_uninit_slice)]
#![feature(maybe_uninit_uninit_array)]
#![allow(clippy::len_without_is_empty)]
#![allow(clippy::new_without_default)]

pub mod active_block;
pub mod active_segment;
pub mod byte_buffer;
pub mod compression;
pub mod constants;
pub mod field_type;
pub mod id;
pub mod kafka;
pub mod mem_list;
pub mod sample;
pub mod segment;
pub mod snapshot;
pub mod source;
#[cfg(test)]
pub mod test_utils;
pub mod tsdb;
pub mod utils;
pub mod writer;
pub mod snapshotter;
pub mod rdtsc;
pub mod counters;
