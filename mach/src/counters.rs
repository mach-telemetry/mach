
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

pub static METADATA_LIST_FLUSHER_CYCLES: Counter = Counter::new();
pub static DATA_BLOCK_FLUSHER_CYCLES: Counter = Counter::new();
pub static WRITER_CYCLES: Counter = Counter::new();

pub fn cpu_cycles() -> usize {
    let x = METADATA_LIST_FLUSHER_CYCLES.load() + 
    DATA_BLOCK_FLUSHER_CYCLES.load() + 
    WRITER_CYCLES.load();
    x as usize
}

pub fn reset_cpu_cycles() {
    METADATA_LIST_FLUSHER_CYCLES.reset();
    DATA_BLOCK_FLUSHER_CYCLES.reset();
    WRITER_CYCLES.reset();
}

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
