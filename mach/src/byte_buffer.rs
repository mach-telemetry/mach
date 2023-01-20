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

use std::io::{self, Write};

pub struct ByteBuffer<'a> {
    len: usize,
    source: &'a mut [u8],
}

impl<'a> ByteBuffer<'a> {
    pub fn new(len: usize, source: &'a mut [u8]) -> Self {
        ByteBuffer { len, source }
    }

    pub fn extend_from_slice(&mut self, src: &[u8]) {
        let start = self.len;
        let end = self.len + src.len();
        self.source[start..end].copy_from_slice(src);
        self.len = end;
    }

    pub fn push(&mut self, byte: u8) {
        self.source[self.len] = byte;
        self.len += 1;
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn set_len(&mut self, sz: usize) {
        self.len = sz
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.source[..self.len]
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.source[..self.len]
    }

    pub fn all_slice(&self) -> &[u8] {
        self.source
    }

    pub fn all_mut_slice(&mut self) -> &mut [u8] {
        &mut self.source[..]
    }

    pub fn remaining(&mut self) -> &mut [u8] {
        &mut self.source[self.len..]
    }

    pub fn resize(&mut self, new_len: usize, item: u8) {
        if new_len < self.len {
            self.len = new_len;
        } else {
            self.source[self.len..new_len].fill(item);
            self.len = new_len;
        }
    }
}

impl<'a> Write for ByteBuffer<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        unimplemented!();
    }
}
