use crate::{
    active_block::*,
    compression::Compression,
    id::SeriesId,
    //persistent_list::{self, List, ListBuffer},
    persistent_list::{self, List, ListSnapshot},
    snapshot::Snapshot,
    //reader::Snapshot,
    segment::{
        self, FlushSegment, FullSegment, Segment, SegmentSnapshot, WriteSegment,
    },
    tags::Tags,
    utils::wp_lock::WpLock,
    durable_queue::QueueConfig,
};
use serde::*;
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

#[derive(PartialEq, Eq, Copy, Clone, Serialize, Deserialize, Debug)]
pub enum Types {
    U64 = 0,
    F64 = 1,
    Bytes = 2,
}

impl Types {
    pub fn to_u8(self) -> u8 {
        self as u8
    }

    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::U64,
            1 => Self::F64,
            2 => Self::Bytes,
            _ => unimplemented!(),
        }
    }
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
    config: SeriesConfig,
    queue: QueueConfig,
    segment: Segment,
    list: List,
}

impl Series {
    pub fn new(config: SeriesConfig, queue: QueueConfig, list: List) -> Self {
        assert_eq!(config.nvars, config.compression.len());
        Self {
            segment: segment::Segment::new(config.seg_count, config.nvars, config.types.as_slice()),
            config,
            list,
            queue,
        }
    }

    pub fn segment_snapshot(&self) -> Result<SegmentSnapshot, Error> {
        Ok(self.segment.snapshot()?)
    }

    pub fn snapshot(&self) -> Result<Snapshot, Error> {
        let segment = self.segment.snapshot()?;
        let list = self.list.snapshot()?;
        Ok(Snapshot::new(segment, list))
    }

    //pub fn snapshot(&self) -> Result<Snapshot, Error> {
    //    let segment = self.segment.snapshot()?;
    //    let list = self.list.snapshot()?;
    //    Ok(Snapshot::new(segment, list))
    //}

    pub fn config(&self) -> &SeriesConfig {
        &self.config
    }

    pub fn queue(&self) -> &QueueConfig {
        &self.queue
    }

    pub fn compression(&self) -> &Compression {
        &self.config.compression
    }

    pub fn segment(&self) -> &Segment {
        &self.segment
    }

    pub fn list(&self) -> &List {
        &self.list
    }
}
