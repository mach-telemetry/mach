use crate::{active_segment::ActiveSegment, active_block::ActiveBlock, mem_list::metadata_list::MetadataList, field_type::FieldType, id::SourceId};

#[derive(Clone)]
pub struct SourceConfig {
    pub id: SourceId,
    pub types: Vec<FieldType>,
    //pub compression: Compression,
}

#[derive(Clone)]
pub struct Source {
    pub config: SourceConfig,
    pub active_segment: ActiveSegment,
    pub active_block: ActiveBlock,
    pub metadata_block: MetadataList,
}
