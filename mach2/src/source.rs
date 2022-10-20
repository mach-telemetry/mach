use crate::{
    id::SourceId,
    field_type::FieldType,
    active_segment::ActiveSegment,
};

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


