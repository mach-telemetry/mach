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
    tags: Tags,
    segment: Segment,
    list: List,
    compression: Compression,
    types: Vec<Types>,
}

impl Series {
    pub fn new(conf: SeriesConfig, list: List) -> Self {
        let SeriesConfig {
            tags,
            compression,
            nvars,
            seg_count,
            types,
        } = conf;
        assert_eq!(nvars, compression.len());

        //let mut heap_pointers: Vec<bool> = types
        //    .iter()
        //    .map(|x| match x {
        //        Types::Bytes => true,
        //        _ => false,
        //    })
        //    .collect();
        Self {
            segment: segment::Segment::new(seg_count, nvars, types.as_slice()),
            tags,
            compression,
            list,
            types,
        }
    }

    //pub fn segment_snapshot(&self) -> Result<SegmentSnapshot, Error> {
    //    Ok(self.segment.snapshot()?)
    //}

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
