use crate::{
    mem_list::{
        data_block::DataBlock,
        read_only::{ReadOnlyMetadataBlock, ReadOnlyMetadataEntry, ReadOnlyDataBlock}
    },
    kafka::KafkaEntry,
    constants::METADATA_BLOCK_SZ,
};

use std::{
    sync::{Arc, RwLock, atomic::{AtomicUsize, Ordering::SeqCst}},
    cell::UnsafeCell,
    mem::MaybeUninit,
};

pub struct MetadataListWriter {
    inner: Arc<InnerMetadataList>
}

impl MetadataListWriter {
    pub fn push(&self, data_block: DataBlock, min: u64, max: u64) {
        assert!(min <= max);
        let time_range = TimeRange {min, max};
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

    pub fn read(&self) -> Result<ReadOnlyMetadataBlock, &str> {
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

    fn read(&self) -> Result<ReadOnlyMetadataBlock, &str> {
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
pub struct MetadataEntry {
    data_block: DataBlock,
    time_range: TimeRange,
}

struct Inner {
    block: [MaybeUninit<MetadataEntry>; METADATA_BLOCK_SZ],
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
        let entry = MetadataEntry {
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
        // Make a copy of the current data
        let block: [MetadataEntry; METADATA_BLOCK_SZ] = unsafe {
            let x: &[MetadataEntry; METADATA_BLOCK_SZ]= MaybeUninit::slice_assume_init_ref(&self.block[..]).try_into().unwrap();
            x.clone()
        };

        // Make current data previous and setup metadata block
        let previous = Arc::new(self.previous.read().unwrap().clone());
        let data_metadata_block = DataMetadataBlock { block, previous };
        let inner_block = InnerMetadataBlock::Data(data_metadata_block);
        let metadata_block = MetadataBlock { inner: Arc::new(RwLock::new(inner_block)) };

        // Write metadata block into previous
        let mut write_guard = self.previous.write().unwrap();
        *write_guard = metadata_block;

        // reset to zero
        self.len.store(0, SeqCst);
    }

    fn read(&self) -> ReadOnlyMetadataBlock {
        unimplemented!()
        //let len = self.len.load(SeqCst);
        //let block: &[MetadataEntry] = unsafe {
        //    MaybeUninit::slice_assume_init_ref(&self.block[..len])
        //};
        //let previous = self.previous.read().unwrap().clone();

        //ReadOnlyMetadataList::new(block, previous)
    }
}

#[derive(Copy, Clone)]
struct TimeRange {
    min: u64,
    max: u64,
}

#[derive(Clone)]
pub struct MetadataBlock {
    inner: Arc<RwLock<InnerMetadataBlock>>
}

impl MetadataBlock {
    fn read_only(&self, mut blocks: Vec<ReadOnlyMetadataEntry>) -> ReadOnlyMetadataBlock {
        let read_guard = self.inner.read().unwrap();
        match &*read_guard {

            // Reached the end of the list. Create a new readonly metadata block
            InnerMetadataBlock::Kafka(x) => {
                ReadOnlyMetadataBlock::new(blocks, x.block.clone(), x.time_range.min, x.time_range.max)
            },

            // Somewhere in the middle of the list, take all the entries and push into the vector,
            // then continue traversing to next metadata block via recursion
            // This will terminate because the first Metadata block was initialized as a kafka
            // entry
            InnerMetadataBlock::Data(x) => {
                for entry in x.block.iter() {
                    let data_block = ReadOnlyDataBlock::from(&entry.data_block);
                    let entry = ReadOnlyMetadataEntry::new(data_block, entry.time_range.min, entry.time_range.max);
                    blocks.push(entry);
                }
                let previous_cloned = x.previous.clone();
                drop(read_guard); // need to drop guard here because traversal would lock out all accessors
                previous_cloned.read_only(blocks)
            },
        }
    }
}

struct KafkaMetadataBlock {
    block: KafkaEntry,
    time_range: TimeRange,
}

struct DataMetadataBlock {
    block: [MetadataEntry; METADATA_BLOCK_SZ],
    previous: Arc<MetadataBlock>
}

enum InnerMetadataBlock {
    Kafka(KafkaMetadataBlock),
    Data(DataMetadataBlock)
}
