use crate::{
    chunk::ReadChunk,
    compression::{Compression, DecompressBuffer},
    segment::{ReadBuffer, ReadSegment},
};
use std::sync::Arc;

struct ReadSet {
    segments: Arc<ReadSegment>,
    chunk: Arc<ReadChunk>,
    buffer: DecompressBuffer,
    state: usize,
    offset: usize,
}

impl ReadSet {
    fn new(segments: Arc<ReadSegment>, chunk: Arc<ReadChunk>) -> Self {
        ReadSet {
            segments,
            chunk,
            buffer: DecompressBuffer::new(),
            state: 0,
            offset: 0,
        }
    }

    fn next_segment(&mut self) {
        match (self.state, self.offset) {
            (0, x) => {
                if x < self.segments.len() {
                    self.offset += 1;
                } else {
                    self.state += 1;
                    self.offset = 0;
                }
            }
            (1, x) => {
                self.buffer.clear();
                Compression::decompress(self.chunk[x].bytes(), &mut self.buffer).unwrap();
                self.offset += 1;
            }
            (_, _) => unimplemented!(),
        }
    }

    fn timestamps(&self) -> &[u64] {
        match (self.state, self.offset) {
            (0, x) => self.segments[x].timestamps(),
            (1, _) => self.buffer.timestamps(),
            (_, _) => unimplemented!(),
        }
    }

    fn variable(&self, id: usize) -> &[[u8; 8]] {
        match (self.state, self.offset) {
            (0, x) => self.segments[x].variable(id),
            (1, _) => self.buffer.variable(id),
            (_, _) => unimplemented!(),
        }
    }
}

//pub struct ReadBuffer {
//    pub id: usize,
//    pub len: usize,
//    pub ts: [u64; 256],
//    pub data: Vec<[u8; 8]>,
//}
//
//struct InnerSample {
//    ts: u64,
//    values: Vec<[u8; 8]>,
//}
//
//pub struct ReadSet {
//    inner_sample: InnerSample,
//    segments: Arc<ReadSegment>,
//    // segments: ...,
//    // chunk: ...,
//    // list: ...,
//}
//
//impl ReadSet {
//}
//
//
//struct InnerStride {
//    ts: Vec<u64>,
//    values: Vec<[u8; 8]>,
//}
//
//
