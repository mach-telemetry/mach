use dashmap::DashMap;
use crate::mem_list::block::Block;
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

pub struct BlockList {
    head: AtomicU64,
    tail: AtomicU64,
    map: DashMap<u64, Block>,
}

impl BlockList {
    pub fn new() -> Self {
        BlockList {
            head: AtomicU64::new(u64::MAX),
            tail: AtomicU64::new(u64::MAX),
            map: DashMap::new(),
        }
    }

    pub fn push(&self, block: Block) {
        let key = self.head.load(SeqCst) + 1;
        self.map.insert(key, block);
        self.head.store(key, SeqCst);
    }
}

