use crate::{
    compression::Compression,
    id::SeriesId,
    //durable_queue::QueueConfig,
    //persistent_list::{self, List, ListBuffer},
    mem_list::{BlockList, SourceBlockList},
    //persistent_list,
    //reader::Snapshot,
    segment::{self, Segment},
    snapshot::Snapshot,
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
pub enum FieldType {
    I64 = 0,
    U64 = 1,
    F64 = 2,
    Bytes = 3,
    Timestamp = 4,
    //U32 = 5,
}

impl FieldType {
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
            //5 => Self::U32,
            _ => unimplemented!(),
        }
    }
}

#[derive(Clone)]
pub struct SeriesConfig {
    pub id: SeriesId,
    pub types: Vec<FieldType>,
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
    pub fn new(
        config: SeriesConfig,
        block_list: Arc<BlockList>,
        source_block_list: Arc<SourceBlockList>,
    ) -> Self {
        assert_eq!(config.nvars, config.compression.len());
        Self {
            segment: segment::Segment::new(config.seg_count, config.types.as_slice()),
            config,
            block_list,
            source_block_list,
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        let active_segment = match self.segment.snapshot() {
            Ok(x) => Some(x.inner),
            Err(_) => None,
        };
        let active_block = match self.block_list.snapshot() {
            Ok(x) => Some(x),
            Err(_) => None,
        };
        let mut source_blocks = None;
        //let _start = std::time::Instant::now();
        while source_blocks.is_none() {
            if let Ok(blocks) = self.source_block_list.snapshot() {
                source_blocks = Some(blocks);
            }
            //if std::time::Instant::now() - start >= std::time::Duration::from_secs(1) {
            //    source_blocks = Some(self.source_block_list.periodic_snapshot());
            //}
        }
        //let historical_blocks = HISTORICAL_BLOCKS.snapshot(self.config.id);

        //let list = self.list.snapshot()?;
        //Ok(Snapshot::new(segment, list))
        Snapshot {
            active_segment,
            active_block,
            source_blocks: source_blocks.unwrap(),
            id: self.config.id,
            //historical_blocks,
        }
    }
}
