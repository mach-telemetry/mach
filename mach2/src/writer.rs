use crate::{
    active_block::{ActiveBlock, ActiveBlockWriter},
    active_segment::{ActiveSegment, ActiveSegmentWriter},
    mem_list::metadata_list::MetadataList,
    sample::SampleType,
    source::{Source, SourceConfig, SourceId},
};
use dashmap::DashMap;
use serde::*;
use std::convert::From;
use std::ops::Deref;
use std::sync::Arc;

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

pub struct Writer {
    source_table: Arc<DashMap<SourceId, Source>>, // Global Source Table
    segments: Vec<ActiveSegmentWriter>,
    source_configs: Vec<SourceConfig>,
    active_block: ActiveBlock,
    active_block_writer: ActiveBlockWriter,
}

impl Writer {
    pub fn new(source_table: Arc<DashMap<SourceId, Source>>) -> Self {
        let (active_block, active_block_writer) = ActiveBlock::new();
        Self {
            source_table,
            segments: Vec::new(),
            source_configs: Vec::new(),
            active_block,
            active_block_writer,
        }
    }

    pub fn push(&mut self, id: SourceRef, ts: u64, sample: &[SampleType]) {
        let idx = (*id) as usize;
        let seg = &mut self.segments[idx];
        if seg.push(ts, sample).is_full() {
            let segment_ref = seg.as_segment_ref();
            let conf = &self.source_configs[idx];
            self.active_block_writer
                .push(conf.id, segment_ref, &conf.compression);
        }
    }

    pub fn add_source(&mut self, config: SourceConfig) -> SourceRef {
        let source_id = config.id;
        let (metadata_list, metadata_list_writer) = MetadataList::new();
        let (active_segment, active_segment_writer) = ActiveSegment::new(config.types.as_slice());
        self.active_block
            .add_source(source_id, metadata_list_writer);
        let source = Source {
            config: config.clone(),
            active_segment,
            metadata_list,
            active_block: self.active_block.clone(),
        };
        self.source_table.insert(source_id, source);
        self.segments.push(active_segment_writer);
        self.source_configs.push(config);
        SourceRef((self.segments.len() - 1) as u64)
    }
}
