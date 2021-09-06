pub mod file;

//pub const KEYSZ: usize = 24;
//pub const BLOCKSZ: usize = 8192;

pub struct BlockError(&'static str);

pub struct BlockKey {
    pub id: u64,
    pub mint: u64,
    pub maxt: u64,
}

pub trait BlockStore<W, R>: Clone
where
    W: BlockWriter,
    R: BlockReader,
{
    fn writer(&self) -> W;
    fn reader(&self, q: BlockKey) -> R;
}

pub trait BlockWriter {
    fn write_block(&mut self, k: BlockKey, d: &[u8]) -> Result<(), BlockError>;
}

pub trait BlockReader {
    fn next_block(&mut self) -> Option<&[u8]>;
}
