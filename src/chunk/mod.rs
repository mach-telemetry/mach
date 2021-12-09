mod chunk;

use crate::backend::fs::{HEADERSZ, TAILSZ};
pub use chunk::*;

pub type FileChunk = Chunk<HEADERSZ, TAILSZ>;
pub type WriteFileChunk = WriteChunk<HEADERSZ, TAILSZ>;
