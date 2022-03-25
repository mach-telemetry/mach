use crate::{
    constants::*, id::SeriesId, compression::DecompressBuffer, persistent_list::{self, ListSnapshot, ListSnapshotReader, ChunkReader}, runtime::RUNTIME, sample::Type,
    segment::{SegmentSnapshot, ReadBuffer}, series::{Types, Series},
    utils::bytes::Bytes,
};
use serde::*;
use bincode::{serialize_into, deserialize_from};
use std::convert::TryInto;

pub enum Error {
    PersistentList(persistent_list::Error)
}

impl From<persistent_list::Error> for Error {
    fn from(item: persistent_list::Error) -> Self {
        Error::PersistentList(item)
    }
}


pub enum SnapshotItem<'a> {
    Buffer(&'a ReadBuffer),
    Compressed(&'a DecompressBuffer),
}

impl<'a> SnapshotItem<'a> {
    fn get_timestamps(&self) -> &[u64] {
        match self {
            Self::Buffer(x) => x.timestamps(),
            Self::Compressed(x) => x.timestamps()
        }
    }

    fn get_field(&self, i: usize) -> (Types, &[[u8; 8]]) {
        match self {
            Self::Buffer(x) => x.variable(i),
            Self::Compressed(x) => x.variable(i)
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Snapshot {
    segments: SegmentSnapshot,
    list: ListSnapshot,
}

impl Snapshot {
    pub fn new(segments: SegmentSnapshot, list: ListSnapshot) -> Self {
        Snapshot { segments, list }
    }

    pub fn to_bytes(self) -> Vec<u8> {
        let mut v = Vec::new();
        serialize_into(&mut v, &self).unwrap();
        v
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        deserialize_from(bytes).unwrap()
    }
}

pub struct SnapshotReader<R: ChunkReader> {
    segments: SegmentSnapshot,
    list: ListSnapshotReader,
    current_buffer: usize,
    current_compressed: usize,
    local_buffer: Vec<u8>,
    decompress_buf: DecompressBuffer,
    reader: R,
}

impl<R: ChunkReader> SnapshotReader<R> {
    pub fn new(snapshot: Snapshot, reader: R) -> Self {
        Self {
            segments: snapshot.segments,
            list: ListSnapshotReader::new(snapshot.list),
            current_buffer: 0,
            current_compressed: 0,
            local_buffer: Vec::new(),
            decompress_buf: DecompressBuffer::new(),
            reader,
        }
    }

    pub fn next_segment(&mut self) -> Result<Option<SnapshotItem>, Error> {
        if self.current_buffer < self.segments.len() {
            let idx = self.current_buffer;
            self.current_buffer += 1;
            Ok(Some(SnapshotItem::Buffer(&self.segments[idx])))
        } else {
            let res = match self.list.next_segment(&mut self.reader)? {
                Some(x) => Some(SnapshotItem::Compressed(x)),
                None => None
            };
            Ok(res)
        }
    }
}

