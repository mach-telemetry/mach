use crate::constants::*;
use crate::mach::id::SeriesId;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use ref_thread_local::{ref_thread_local, RefThreadLocal};

ref_thread_local! {
    static managed RNG: ChaCha8Rng = ChaCha8Rng::seed_from_u64(PARAMETERS.query_rand_seed);
}

const MICROS_IN_SECOND: u64 = 1_000_000;

pub struct SimpleQuery {
    pub source: SeriesId,
    pub start: u64,
    pub end: u64,
    pub from_now: u64,
}

impl SimpleQuery {
    pub fn new_relative_to(relative_to: u64) -> Self {
        let now = relative_to;
        let mut rng = RNG.borrow_mut();
        let source = SeriesId(rng.gen_range(0..PARAMETERS.source_count));
        let from_now: u64 = rng.gen_range(PARAMETERS.query_min_delay..PARAMETERS.query_max_delay);
        let start = now - from_now * MICROS_IN_SECOND;
        let end = start
            - rng.gen_range(PARAMETERS.min_query_duration..PARAMETERS.max_query_duration)
                * MICROS_IN_SECOND;
        SimpleQuery { source, start, end, from_now }
    }
}
