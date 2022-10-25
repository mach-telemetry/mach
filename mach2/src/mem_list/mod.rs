use std::sync::{Arc, RwLock};

pub struct ActiveBlock {}

pub struct MetadataBlock {
}

enum InnerMetadataBlock {
    Offset,
    Block(Arc<[u8]>)
}

