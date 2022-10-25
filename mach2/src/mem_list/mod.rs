use std::sync::{Arc, RwLock};
use crate::kafka::KafkaEntry;
mod data_block;

pub struct ActiveBlock {}

pub struct MetadataBlock {
}

enum InnerMetadataBlock {
    Offset(KafkaEntry),
    Block(Arc<[u8]>)
}

