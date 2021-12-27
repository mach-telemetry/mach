use crate::persistent_list::{
    vector_writer::{Vector, VectorWriter, HEADERSZ},
    chunk_trait::{Chunker, ChunkWriter},
    memory::{Buffer, BufferWriter},
};
use std::sync::{Arc, Mutex};

pub struct VectorList {
    buffer: Buffer,
    chunker: Vector,
}

pub struct VectorListWriter {
    buffer: BufferWriter,
    chunker: VectorWriter,
}

impl VectorList {
    fn new(inner: Arc<Mutex<Vec<Box<[u8]>>>>) -> Self {
        let chunker = Vector::new(inner);
        let buffer = Buffer::new(HEADERSZ);
        VectorList {
            buffer,
            chunker,
        }
    }

    fn writer(&self) -> VectorListWriter {
        VectorListWriter {
            buffer: self.buffer.writer(),
            chunker: self.chunker.writer().unwrap(),
        }
    }
}
