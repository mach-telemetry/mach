use crate::tsdb::{Dt, SeriesId};

pub mod file;

//pub const KEYSZ: usize = 24;
pub const BLOCKSZ: usize = 8192;

pub trait BlockStore<W, R>: Clone
where
    W: BlockWriter,
    R: BlockReader,
{
    fn writer(&self) -> W;
    fn reader(&self, id: SeriesId) -> Option<R>;
}

pub trait BlockWriter {
    fn write_block(
        &mut self,
        series_id: SeriesId,
        mint: Dt,
        maxt: Dt,
        d: &[u8],
    ) -> Result<(), &'static str>;
}

pub trait BlockReader {
    fn set_range(&mut self, mint: Dt, maxt: Dt);
    fn next_block(&mut self) -> Option<&[u8]>;
}
