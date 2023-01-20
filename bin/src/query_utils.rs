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

use crate::constants::*;
use mach::source::SourceId;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use ref_thread_local::{ref_thread_local, RefThreadLocal};

ref_thread_local! {
    static managed RNG: ChaCha8Rng = ChaCha8Rng::seed_from_u64(PARAMETERS.query_rand_seed);
}

const MICROS_IN_SECOND: u64 = 1_000_000;

pub struct SimpleQuery {
    pub source: SourceId,
    pub start: u64,
    pub end: u64,
    pub from_now: u64,
}

impl SimpleQuery {
    pub fn new_relative_to(relative_to: u64) -> Self {
        let now = relative_to;
        let mut rng = RNG.borrow_mut();
        let source = SourceId(rng.gen_range(0..PARAMETERS.source_count));
        let from_now: u64 = rng.gen_range(PARAMETERS.query_min_delay..PARAMETERS.query_max_delay);
        let start = now - from_now * MICROS_IN_SECOND;
        let end = start
            - rng.gen_range(PARAMETERS.query_min_duration..PARAMETERS.query_max_duration)
                * MICROS_IN_SECOND;
        SimpleQuery {
            source,
            start,
            end,
            from_now,
        }
    }
}
