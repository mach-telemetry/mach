use crate::{
    compression::Compression,
    //durable_queue::QueueConfig,
    //persistent_list::{self, List, ListBuffer},
    mem_list::{SourceBlockList, BlockList},
    //persistent_list,
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
    //PersistentList(persistent_list::Error),
}

//impl From<persistent_list::Error> for Error {
//    fn from(item: persistent_list::Error) -> Self {
//        Error::PersistentList(item)
//    }
//}

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
    pub block_list: Arc<BlockList>,
    pub source_block_list: Arc<SourceBlockList>,
}

impl Series {
    pub fn new(config: SeriesConfig, block_list: Arc<BlockList>, source_block_list: Arc<SourceBlockList>) -> Self {
        assert_eq!(config.nvars, config.compression.len());
        Self {
            segment: segment::Segment::new(config.seg_count, config.types.as_slice()),
            config,
            block_list,
            source_block_list,
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        let active_segment = self.segment.snapshot().unwrap().inner;
        let active_block = self.block_list.snapshot().unwrap();
        let source_blocks = self.source_block_list.snapshot().unwrap();
        //let list = self.list.snapshot()?;
        //Ok(Snapshot::new(segment, list))
        Snapshot {
            active_segment,
            active_block,
            source_blocks,
            id: self.config.id,
        }
    }
}
