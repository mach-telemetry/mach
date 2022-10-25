use std::sync::{Arc, RwLock};

pub struct DataBlock {
    inner: Mutex<InnerDataBlock>
}

enum InnerDataBlock {
    Offset,
    Data,
}


