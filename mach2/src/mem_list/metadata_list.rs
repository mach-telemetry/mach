use crate::{
    mem_list::data_block::DataBlock,
    kafka::KafkaEntry,
    constants::METADATA_BLOCK_SZ,
};

use std::{
    sync::{Arc, RwLock, atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst}},
    cell::UnsafeCell,
    mem::MaybeUninit,
};

pub struct MetadataListWriter {
    inner: Arc<InnerMetadataList>
}

impl MetadataListWriter {
    pub fn push(&self, data_block: DataBlock, time_range: TimeRange) {
        match self.inner.push(data_block, time_range) {
            PushStatus::Full => self.inner.reset(),
            PushStatus::Ok => {}
        }
    }
}

pub struct MetadataList {
    inner: Arc<InnerMetadataList>
}

impl MetadataList {
    pub fn new() -> (Self, MetadataListWriter) {
        let inner = Arc::new(InnerMetadataList::new());
        (
            MetadataList { inner: inner.clone() },
            MetadataListWriter { inner },
        )
    }

    pub fn read(&self) -> Result<ReadOnlyMetadataList, &str> {
        self.inner.read()
    }
}

struct InnerMetadataList {
    version: AtomicUsize,
    inner: UnsafeCell<Inner>,
}

impl InnerMetadataList {

    fn new() -> Self {
        Self {
            version: AtomicUsize::new(0),
            inner: UnsafeCell::new(Inner::new()),
        }
    }

    // Safety: Needs wrapper to ensure only one alias
    fn push(&self, data_block: DataBlock, time_range: TimeRange) -> PushStatus {
        // Safety: Push races with read but does not affect correctness because of atomics
        unsafe { (&mut *self.inner.get()).push(data_block, time_range) }
    }

    fn reset(&self) {
        self.version.fetch_add(1, SeqCst);
        // Safety: reset races with read re: correctness. Versioning ensures read would fail if
        // this happens
        unsafe { (&mut *self.inner.get()).reset() }
        self.version.fetch_add(1, SeqCst);
    }

    fn read(&self) -> Result<ReadOnlyMetadataList, &str> {
        let version = self.version.load(SeqCst);
        // Safety: this is an & reference, not &mut. Could race with push/reset calls which are
        // unsafe. Need to ensure that 
        let result = unsafe {
            (&*self.inner.get()).read()
        };
        if version != self.version.load(SeqCst) {
            Err("Failed to make snapshot of metadatalist")
        } else {
            Ok(result)
        }
    }
}

enum PushStatus {
    Ok,
    Full,
}

#[derive(Clone)]
struct Entry {
    data_block: DataBlock,
    time_range: TimeRange,
}

struct Inner {
    block: [MaybeUninit<Entry>; METADATA_BLOCK_SZ],
    len: AtomicUsize,
    fully_initted: bool,
    previous: RwLock<MetadataBlock>
}

impl Inner {

    fn new() -> Self {
        let kafka_entry = KafkaMetadataBlock {
            block: KafkaEntry::new(),
            time_range: TimeRange { min: u64::MAX, max: 0 },
        };
        let inner_block = InnerMetadataBlock::Kafka(kafka_entry);
        let block = MetadataBlock {
            inner: Arc::new(RwLock::new(inner_block))
        };
        let previous = RwLock::new(block);
        Self {
            block: MaybeUninit::uninit_array(),
            len: AtomicUsize::new(0),
            fully_initted: false,
            previous,
        }
    }

    fn push(&mut self, data_block: DataBlock, time_range: TimeRange) -> PushStatus {
        let len = self.len.load(SeqCst);
        let entry = Entry {
            data_block,
            time_range,
        };

        // Drop in place if this item has been initialized
        if self.fully_initted {
            // Safety
            // We now know this is fully initted
            unsafe { self.block[len].assume_init_drop(); }
        }

        self.block[len].write(entry);

        let result = if len + 1 == METADATA_BLOCK_SZ {
            self.fully_initted = true;
            PushStatus::Full
        } else {
            PushStatus::Ok
        };

        self.len.fetch_add(1, SeqCst);
        result
    }

    fn reset(&mut self) {
        self.len.store(0, SeqCst);
    }

    fn read(&self) -> ReadOnlyMetadataList {
        let len = self.len.load(SeqCst);
        let block: Box<[Entry]> = unsafe {
            MaybeUninit::slice_assume_init_ref(&self.block[..len]).iter().cloned().collect()
        };
        let previous = self.previous.read().unwrap().clone();

        ReadOnlyMetadataList {
            block,
            len,
            previous,
        }
    }
}

struct ReadOnlyMetadataList {
    block: Box<[Entry]>,
    len: usize,
    previous: MetadataBlock,
}

#[derive(Copy, Clone)]
struct TimeRange {
    min: u64,
    max: u64,
}

#[derive(Clone)]
struct MetadataBlock {
    inner: Arc<RwLock<InnerMetadataBlock>>
}

struct KafkaMetadataBlock {
    block: KafkaEntry,
    time_range: TimeRange,
}

struct DataMetadataBlock {
    block: [Entry; 256],
    previous: Arc<MetadataBlock>
}

enum InnerMetadataBlock {
    Kafka(KafkaMetadataBlock),
    Data(DataMetadataBlock)
}
