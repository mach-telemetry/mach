use crate::{
    segment::SegmentSnapshot,
    mem_list::{ReadOnlyBlock, SourceBlocks},
};
use std::sync::Arc;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Snapshot {
    active_segment: SegmentSnapshot,
    active_block: ReadOnlyBlock,
    source_blocks: SourceBlocks,
}
