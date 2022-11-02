use crate::{
    active_block::ActiveBlock, active_segment::ActiveSegment, compression::Compression,
    field_type::FieldType, mem_list::metadata_list::MetadataList,
};
use serde::*;
use std::ops::Deref;

#[derive(Copy, Clone, Eq, Debug, PartialEq, Hash, Serialize, Deserialize)]
pub struct SourceId(pub u64);

impl Deref for SourceId {
    type Target = u64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<u64> for SourceId {
    fn from(id: u64) -> Self {
        SourceId(id as u64)
    }
}

#[derive(Clone)]
pub struct SourceConfig {
    pub id: SourceId,
    pub types: Vec<FieldType>,
    pub compression: Compression,
}

//pub struct SourceWriter {
//    config: SourceConfig,
//    active_segment: ActiveSegmentWriter,
//    active_block: ActiveBlockWriter,
//}

//impl SourceWriter {
//    pub fn push(&mut self, ts: u64, items: &[SampleType]) {
//        match self.active_segment.push(ts, items) {
//            active_segment::PushStatus::Full => {
//                let segment_reference = self.active_segment.as_segment_ref();
//                let id = self.config.id;
//                let compression = &self.config.compression;
//                self.active_block.push(id, segment_reference, compression);
//                self.active_segment.reset();
//            }
//            active_segment::PushStatus::Ok => {},
//            active_segment::PushStatus::ErrorFull => unreachable!(),
//        }
//    }
//}

#[derive(Clone)]
pub struct Source {
    pub config: SourceConfig,
    pub active_segment: ActiveSegment,
    pub active_block: ActiveBlock,
    pub metadata_list: MetadataList,
}
