use crate::{
    compression::Compression,
    id::SeriesId,
    persistent_list::{self, List, ListBuffer},
    reader::Snapshot,
    segment::{
        self, FlushSegment, FullSegment, ReadSegment, Segment, SegmentSnapshot, WriteSegment,
    },
    tags::Tags,
};
use std::sync::Arc;

#[derive(Debug)]
pub enum Error {
    Segment(segment::Error),
    PersistentList(persistent_list::Error),
}

impl From<persistent_list::Error> for Error {
    fn from(item: persistent_list::Error) -> Self {
        Error::PersistentList(item)
    }
}

impl From<segment::Error> for Error {
    fn from(item: segment::Error) -> Self {
        Error::Segment(item)
    }
}

#[derive(Copy, Clone)]
pub enum Types {
    U64,
    F64,
    Bytes,
}

#[derive(Clone)]
pub struct SeriesConfig {
    pub tags: Tags,
    pub types: Vec<Types>,
    pub compression: Compression,
    pub seg_count: usize,
    pub nvars: usize,
}

#[derive(Clone)]
pub struct Series {
    tags: Tags,
    segment: Segment,
    list: List,
    compression: Compression,
    types: Vec<Types>,
}

impl Series {
    pub fn new(conf: SeriesConfig, buffer: ListBuffer) -> Self {
        let SeriesConfig {
            tags,
            compression,
            nvars,
            seg_count,
            types,
        } = conf;
        assert_eq!(nvars, compression.len());

        let mut heap_pointers: Vec<bool> = types
            .iter()
            .map(|x| match x {
                Types::Bytes => true,
                _ => false,
            })
            .collect();
        Self {
            segment: segment::Segment::new(seg_count, nvars, heap_pointers.as_slice()),
            tags,
            compression,
            list: List::new(buffer),
            types,
        }
    }

    pub fn segment_snapshot(&self) -> Result<SegmentSnapshot, Error> {
        Ok(self.segment.snapshot()?)
    }

    pub fn snapshot(&self) -> Result<Snapshot, Error> {
        let segment = self.segment.snapshot()?;
        let list = self.list.read()?;
        Ok(Snapshot::new(segment, list))
    }

    pub fn compression(&self) -> &Compression {
        &self.compression
    }

    pub fn segment(&self) -> &Segment {
        &self.segment
    }

    pub fn list(&self) -> &List {
        &self.list
    }
}
