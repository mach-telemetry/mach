use crate::{
    compression::Compression,
    durable_queue::QueueConfig,
    //persistent_list::{self, List, ListBuffer},
    mem_list::{BlockList, List},
    persistent_list,
    //reader::Snapshot,
    segment::{self, Segment, SegmentSnapshot},
    snapshot::Snapshot,
    id::SeriesId,
};
use serde::*;
use std::sync::Arc;

#[derive(Debug)]
pub enum Error {
    Noop,
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
    I64 = 0,
    U64 = 1,
    F64 = 2,
    Bytes = 3,
    Timestamp = 4,
    U32 = 5,
    //Fixed(usize) = 6,
}

impl Types {
    pub fn to_u8(self) -> u8 {
        self as u8
    }

    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::I64,
            1 => Self::U64,
            2 => Self::F64,
            3 => Self::Bytes,
            4 => Self::Timestamp,
            5 => Self::U32,
            //6 => Fixed(usize),
            _ => unimplemented!(),
        }
    }
}

#[derive(Clone)]
pub struct SeriesConfig {
    pub id: SeriesId,
    pub types: Vec<Types>,
    pub compression: Compression,
    pub seg_count: usize,
    pub nvars: usize,
}

#[derive(Clone)]
pub struct Series {
    pub config: SeriesConfig,
    pub segment: Segment,
    pub list: Arc<List>,
    pub block_list: Arc<BlockList>,
}

impl Series {
    pub fn new(config: SeriesConfig, block_list: Arc<BlockList>) -> Self {
        assert_eq!(config.nvars, config.compression.len());
        Self {
            segment: segment::Segment::new(config.seg_count, config.types.as_slice()),
            list: Arc::new(List::new(config.id)),
            config,
            block_list,
        }
    }

    pub fn segment_snapshot(&self) -> Result<SegmentSnapshot, Error> {
        Ok(self.segment.snapshot()?)
    }

    pub fn snapshot(&self) -> Result<Snapshot, Error> {
        //let segment = self.segment.snapshot()?;
        //let list = self.list.snapshot()?;
        //Ok(Snapshot::new(segment, list))
        Err(Error::Noop)
    }

    //pub fn snapshot(&self) -> Result<Snapshot, Error> {
    //    let segment = self.segment.snapshot()?;
    //    let list = self.list.snapshot()?;
    //    Ok(Snapshot::new(segment, list))
    //}

    //pub fn config(&self) -> &SeriesConfig {
    //    &self.config
    //}

    ////pub fn queue(&self) -> &QueueConfig {
    ////    &self.queue
    ////}

    //pub fn compression(&self) -> &Compression {
    //    &self.config.compression
    //}

    //pub fn segment(&self) -> &Segment {
    //    &self.segment
    //}

    //pub fn list(&self) -> &List {
    //    &self.list
    //}
}
