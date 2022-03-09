use crate::constants::*;
use crate::segment::buffer::*;

pub struct FullSegment<'a> {
    pub len: usize,
    pub nvars: usize,
    pub ts: &'a [u64; SEGSZ],
    pub data: &'a [Column],
    //pub heap: &'a [Vec<u8>],
    //pub heap_pointers: &'a [bool],
}

impl<'a> FullSegment<'a> {
    pub fn timestamps(&self) -> &[u64] {
        &self.ts[..self.len]
    }

    pub fn variable(&self, var: usize) -> &[[u8; 8]] {
        &self.data[var][..self.len]
    }
}
