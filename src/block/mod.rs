use crate::ids::SeriesId;
pub mod file;

//pub const KEYSZ: usize = 24;
pub const BLOCKSZ: usize = 8192;

pub trait BlockStore<W, R>: Clone
where
    W: BlockWriter,
    R: BlockReader,
{
    fn writer(&self) -> W;
    fn reader(&self, series_id: SeriesId) -> Option<R>;
    fn series_ids(&self) -> Vec<SeriesId>; // TODO: this is a not so elegant hack, better to use an iterator but for now, this works
}

pub trait BlockWriter {
    fn write_block(
        &mut self,
        series_id: SeriesId,
        mint: u64,
        maxt: u64,
        d: &[u8],
    ) -> Result<(), &'static str>;
}

pub trait BlockReader {
    fn set_range(&mut self, mint: u64, maxt: u64);
    fn next_block(&mut self, buf: &mut [u8]) -> Option<usize>;
}
