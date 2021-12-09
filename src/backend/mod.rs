pub mod fs;

use crate::flush_buffer::FrozenBuffer;

pub struct ByteEntry<'a> {
    pub mint: u64,
    pub maxt: u64,
    pub bytes: &'a [u8],
}

impl<'a> ByteEntry<'a> {
    pub fn new(mint: u64, maxt: u64, bytes: &'a[u8]) -> Self {
        Self { mint, maxt, bytes }
    }
}

pub struct FlushEntry<'buffer, const H: usize, const T: usize> {
    pub mint: u64,
    pub maxt: u64,
    pub buffer: FrozenBuffer<'buffer, H, T>,
}

impl<'buffer, const H: usize, const T: usize> FlushEntry<'buffer, H, T> {
    pub fn new(mint: u64, maxt: u64, buffer: FrozenBuffer<'buffer, H, T>) -> Self {
        Self { mint, maxt, buffer }
    }
}

//pub enum PushMetadata {
//    FS({
//        offset: u64,
//        file_id: u64,
//        ts_id: u64,
//    }),
//}
