use crate::mem_list::{block_list::BlockList};
use std::sync::{Arc, atomic::{AtomicU64, Ordering::SeqCst}};

pub struct SourceList {
    block_list: Arc<BlockList>,
    head: Arc<AtomicU64>,
}
