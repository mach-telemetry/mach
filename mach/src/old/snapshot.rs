use crate::{
    compression::{self, Compression, DecompressBuffer},
    durable_queue::DurableQueueReader,
    //persistent_list::{self, ListSnapshot, ListSnapshotReader},
    //runtime::RUNTIME,
    segment::{ReadBuffer, SegmentSnapshot, SegmentSnapshotReader},
    series::Types,
};
use bincode::{deserialize_from, serialize_into};
use serde::*;
use std::iter::Rev;
use std::slice;

#[derive(Debug)]
pub enum Error {
    //PersistentList(persistent_list::Error),
    Compression(compression::Error),
}

//impl From<persistent_list::Error> for Error {
//    fn from(item: persistent_list::Error) -> Self {
//        Error::PersistentList(item)
//    }
//}

impl From<compression::Error> for Error {
    fn from(item: compression::Error) -> Self {
        Error::Compression(item)
    }
}

pub type TimestampIterator<'a> = Rev<slice::Iter<'a, u64>>;
pub type FieldIterator<'a> = Rev<slice::Iter<'a, [u8; 8]>>;

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

    pub fn get_field(&self, i: usize) -> (Types, FieldIterator) {
        match self {
            Self::Active(x) => {
                let (t, v) = x.variable(i);
                (t, v.iter().rev())
            }
            Self::Compressed(x) => {
                let (t, v) = x.variable(i);
                (t, v.iter().rev())
            }
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

    pub fn reader(&self, durable_queue: DurableQueueReader) -> SnapshotReader {
        SnapshotReader::new(self, durable_queue)
    }
}

pub struct SnapshotReader {
    segments: SegmentSnapshotReader,
    list: Option<ListSnapshotReader>,
    decompress_buf: DecompressBuffer,
}

impl SnapshotReader {
    pub fn new(snapshot: &Snapshot, durable_queue: DurableQueueReader) -> Self {
        SnapshotReader {
            segments: snapshot.segments.reader(),
            list: snapshot.list.reader(durable_queue).ok(),
            decompress_buf: DecompressBuffer::new(),
        }
    }

    pub fn next_item(&mut self) -> Result<Option<SnapshotItem>, Error> {
        if let Some(r) = self.segments.next_item() {
            return Ok(Some(SnapshotItem::Active(r)));
        }

        if self.list.is_some() {
            if let Some(r) = self.list.as_mut().unwrap().next_item()? {
                let _ = Compression::decompress(r, &mut self.decompress_buf)?;
                return Ok(Some(SnapshotItem::Compressed(&self.decompress_buf)));
            }
        }

        Ok(None)
    }
}
