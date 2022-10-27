use crate::{active_segment::ActiveSegment, field_type::FieldType, id::SourceId};

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
    //pub block_list: Arc<BlockList>,
    //pub source_block_list: Arc<SourceBlockList>,
}
