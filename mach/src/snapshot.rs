use crate::{
    compression::{self, Compression, DecompressBuffer},
    constants::*,
    id::SeriesId,
    persistent_list::{self, ListSnapshot, ListSnapshotReader},
    //runtime::RUNTIME,
    sample::Type,
    segment::{ReadBuffer, SegmentSnapshot, SegmentSnapshotReader},
    series::{Series, Types},
    utils::bytes::Bytes,
    durable_queue::DurableQueueReader,
};
use bincode::{deserialize_from, serialize_into};
use serde::*;
use std::convert::TryInto;
use std::slice;
use std::iter::Rev;

#[derive(Debug)]
pub enum Error {
    PersistentList(persistent_list::Error),
    Compression(compression::Error),
}

impl From<persistent_list::Error> for Error {
    fn from(item: persistent_list::Error) -> Self {
        Error::PersistentList(item)
    }
}

impl From<compression::Error> for Error {
    fn from(item: compression::Error) -> Self {
        Error::Compression(item)
    }
}


pub type TimestampIterator<'a> = Rev<slice::Iter<'a, u64>>;

pub enum SnapshotItem<'a> {
    Active(&'a ReadBuffer),
    Compressed(&'a DecompressBuffer),
}

impl<'a> SnapshotItem<'a> {
    pub fn get_timestamps(&self) -> TimestampIterator {
        match self {
            Self::Active(x) => x.timestamps().iter().rev(),
            Self::Compressed(x) => x.timestamps().iter().rev(),
        }
    }

    pub fn get_field(&self, i: usize) -> (Types, &[[u8; 8]]) {
        match self {
            Self::Active(x) => x.variable(i),
            Self::Compressed(x) => x.variable(i),
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

    pub fn reader(&self, durable_queue: DurableQueueReader) -> Result<SnapshotReader, Error> {
        SnapshotReader::new(self, durable_queue)
    }
}

pub struct SnapshotReader {
    segments: SegmentSnapshotReader,
    list: ListSnapshotReader,
    decompress_buf: DecompressBuffer,
}

impl SnapshotReader {
    pub fn new(snapshot: &Snapshot, durable_queue: DurableQueueReader) -> Result<Self, Error> {
        Ok(SnapshotReader {
            segments: snapshot.segments.reader(),
            list: snapshot.list.reader(durable_queue)?,
            decompress_buf: DecompressBuffer::new(),
        })
    }

    pub fn next_item(&mut self) -> Result<Option<SnapshotItem>, Error> {
        if let Some(r) = self.segments.next_item() {
            Ok(Some(SnapshotItem::Active(r)))
        } else if let Some(r) = self.list.next_item()? {
            let _ = Compression::decompress(r, &mut self.decompress_buf)?;
            Ok(Some(SnapshotItem::Compressed(&self.decompress_buf)))
        } else {
            Ok(None)
        }
    }
}
