use crate::{
    source::{Source, SourceId},
    active_segment::ActiveSegmentWriter,
    active_block::{ActiveBlock, ActiveBlockWriter},
};
use std::sync::Arc;
use std::ops::Deref;
use std::convert::From;
use dashmap::DashMap;
use serde::*;

#[derive(Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct SourceRef(pub u64);

impl Deref for SourceRef {
    type Target = u64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<u64> for SourceRef {
    fn from(id: u64) -> Self {
        SourceRef(id)
    }
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct WriterId(pub u64);
impl Deref for WriterId {
    type Target = u64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<u64> for WriterId {
    fn from(id: u64) -> Self {
        WriterId(id)
    }
}

#[derive(Clone)]
pub struct WriterMetadata {
    pub id: WriterId,
    pub active_block: ActiveBlock,
}

pub struct Writer {
    pub sources: Arc<DashMap<SourceId, Source>>,
    segments: Vec<ActiveSegmentWriter>,
    active_block: ActiveBlockWriter,
}

// impl Writer {
//     pub fn new(sources: Sources) -> Self {
//         Self {
//             sources,
//             segments: Vec::new()
//         }
//     }
// 
//     pub fn add_source(&self, id: SourceConfig) -> SourceRef {
//         let config = self.sources.get(&id).unwrap();
//     }
// }


