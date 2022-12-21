
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

pub static METADATA_LIST_FLUSHER_CYCLES: Counter = Counter::new();
pub static DATA_BLOCK_FLUSHER_CYCLES: Counter = Counter::new();
pub static WRITER_CYCLES: Counter = Counter::new();

pub struct Counter(AtomicU64);

impl Counter {
    pub const fn new() -> Self {
        Counter(AtomicU64::new(0))
    }

    pub fn increment(&self, v: u64) -> u64 {
        self.0.fetch_add(v, SeqCst)
    }

    pub fn decrement(&self, v: u64) -> u64 {
        self.0.fetch_sub(v, SeqCst)
    }

    pub fn load(&self) -> u64 {
        self.0.load(SeqCst)
    }

    pub fn store(&self, v: u64) {
        self.0.store(v, SeqCst)
    }

    pub fn reset(&self) {
        self.store(0)
    }
}
