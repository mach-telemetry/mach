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


use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

pub static METADATA_LIST_FLUSHER_CYCLES: Counter = Counter::new();
pub static DATA_BLOCK_FLUSHER_CYCLES: Counter = Counter::new();
pub static WRITER_CYCLES: Counter = Counter::new();

pub fn cpu_cycles() -> usize {
    let x = METADATA_LIST_FLUSHER_CYCLES.load() + 
    DATA_BLOCK_FLUSHER_CYCLES.load() + 
    WRITER_CYCLES.load();
    x as usize
}

pub fn reset_cpu_cycles() {
    METADATA_LIST_FLUSHER_CYCLES.reset();
    DATA_BLOCK_FLUSHER_CYCLES.reset();
    WRITER_CYCLES.reset();
}

pub struct Counter(AtomicU64);

impl Counter {
    pub const fn new() -> Self {
        Counter(AtomicU64::new(0))
    }

    pub fn increment(&self, v: u64) -> u64 {
        self.0.fetch_add(v, SeqCst)
    }

    pub fn decrement(&self, v: u64) -> u64 {
        self.0.fetch_sub(v, SeqCst)
    }

    pub fn load(&self) -> u64 {
        self.0.load(SeqCst)
    }

    pub fn store(&self, v: u64) {
        self.0.store(v, SeqCst)
    }

    pub fn reset(&self) {
        self.store(0)
    }
}
