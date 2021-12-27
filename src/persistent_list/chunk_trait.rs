use crate::persistent_list::Error;

//pub struct ChunkHead {
//    partition: usize,
//    offset: usize,
//}

pub trait Chunker<W: ChunkWriter, R: ChunkReader> {
    fn writer(&self) -> Result<W, Error>;
    fn reader(&self) -> Result<R, Error>;
}

pub trait ChunkWriter {
    fn write(&mut self, bytes: &[u8]) -> Result<(), Error>;
    fn header(&self) -> &[u8];
}

pub trait ChunkReader {
    fn read_next(&mut self) -> Option<&[u8]>;
}


