use crate::{
    chunk::Chunk,
    segment::Segment,
    compression::Compression,
};

struct MemSeries {
    chunk: Chunk,
    segment: Segment,
}

impl MemSeries {
    fn new(tsid: u64, compression: Compression, b: usize, v: usize) -> Self {
        MemSeries {
            chunk: Chunk::new(tsid, compression),
            segment: Segment::new(b, v),
        }
    }
}
