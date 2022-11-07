use crate::{
    constants::{METADATA_BLOCK_SZ, PARTITIONS},
    kafka::{self, KafkaEntry, Producer},
    mem_list::{
        data_block::DataBlock,
        read_only::{ReadOnlyDataBlock, ReadOnlyMetadataBlock, ReadOnlyMetadataEntry},
    },
};

use crossbeam::channel::{unbounded, Receiver, Sender};
use lazy_static::lazy_static;
use log::{debug, error, info};
use rand::{thread_rng, Rng};
use std::{
    cell::UnsafeCell,
    mem::MaybeUninit,
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc, RwLock,
    },
};

lazy_static! {
    static ref METADATA_BLOCK_WRITER: Sender<MetadataBlock> = {
        kafka::init();
        let (tx, rx) = unbounded();
        std::thread::Builder::new()
            .name("Mach: Metadata Block Kafka Flusher".to_string())
            .spawn(move || {
                flush_worker(rx);
            })
            .unwrap();
        tx
    };
}

fn flush_worker(channel: Receiver<MetadataBlock>) {
    info!("Initing Mach Metadata Block Kafka Flusher");
    let mut partition: i32 = thread_rng().gen_range(0..PARTITIONS);
    let mut counter = 0;
    let mut producer = Producer::new();
    while let Ok(block) = channel.recv() {
        debug!("Metadata Block flusher received a metadata block");
        block.flush(partition, &mut producer);
        if counter % 1000 == 0 {
            partition = thread_rng().gen_range(0..PARTITIONS);
        }
        counter += 1;
    }
    error!("Mach Metadata Block Kafka Flusher Exited");
}

pub struct MetadataListWriter {
    inner: Arc<InnerMetadataList>,
}

impl MetadataListWriter {
    pub fn push(&self, data_block: DataBlock, min: u64, max: u64) {
        assert!(min <= max, "min: {}, max: {}", min, max);
        let time_range = TimeRange { min, max };
        match self.inner.push(data_block, time_range) {
            PushStatus::Full => self.inner.reset(),
            PushStatus::Ok => {}
        }
    }
}

// Safety: It is safe to share MetadataList with multiple threads because of the sync mechansims
// implemented in Inner and the restricted methods of MetadataList. See Above.
unsafe impl Sync for MetadataList {}

// Safety: It is safe to send MetadataList with multiple threads because of the sync mechansims
// implemented in Inner and the restricted methods of MetadataList. See Above.
unsafe impl Send for MetadataList {}

#[derive(Clone)]
pub struct MetadataList {
    inner: Arc<InnerMetadataList>,
}

impl MetadataList {
    pub fn new() -> (Self, MetadataListWriter) {
        let inner = Arc::new(InnerMetadataList::new());
        (
            MetadataList {
                inner: inner.clone(),
            },
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
        unsafe { (*self.inner.get()).push(data_block, time_range) }
    }

    fn reset(&self) {
        self.version.fetch_add(1, SeqCst);
        // Safety: reset races with read re: correctness. Versioning ensures read would fail if
        // this happens
        unsafe { (*self.inner.get()).reset() }
        self.version.fetch_add(1, SeqCst);
    }

    fn read(&self) -> Result<ReadOnlyMetadataBlock, &str> {
        let version = self.version.load(SeqCst);
        // Safety: this is an & reference, not &mut. Could race with push/reset calls which are
        // unsafe. Need to ensure that
        let result = unsafe { (*self.inner.get()).read() };
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

struct Inner {
    block: [MaybeUninit<MetadataEntry>; METADATA_BLOCK_SZ],
    len: AtomicUsize,
    fully_initted: bool,
    previous: RwLock<MetadataBlock>,
}

impl Inner {
    fn new() -> Self {
        let kafka_entry = KafkaMetadataBlock {
            block: KafkaEntry::new(),
            time_range: TimeRange {
                min: u64::MAX,
                max: 0,
            },
        };
        let inner_block = InnerMetadataBlock::Kafka(kafka_entry);
        let block = MetadataBlock {
            inner: Arc::new(RwLock::new(inner_block)),
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
            unsafe {
                self.block[len].assume_init_drop();
            }
        }

        self.block[len].write(entry);

        let result = if len + 1 == METADATA_BLOCK_SZ {
            self.fully_initted = true;
            PushStatus::Full
        } else {
            PushStatus::Ok
        };

        let _l = self.len.fetch_add(1, SeqCst);
        debug!("Pushed data block, metadatalist length: {}", _l);
        result
    }

    fn reset(&mut self) {
        debug!("Resetting metadata list");
        // Make a copy of the current data
        let block: [MetadataEntry; METADATA_BLOCK_SZ] = unsafe {
            let x: &[MetadataEntry; METADATA_BLOCK_SZ] =
                MaybeUninit::slice_assume_init_ref(&self.block[..])
                    .try_into()
                    .unwrap();
            x.clone()
        };

        let time_range = {
            let min = block[0].time_range.min;
            let max = block.last().unwrap().time_range.max;
            TimeRange { min, max }
        };

        // Make current data previous and setup metadata block
        let previous = Arc::new(self.previous.read().unwrap().clone());
        let data_metadata_block = DataMetadataBlock {
            block,
            time_range,
            previous,
        };
        let inner_block = InnerMetadataBlock::Data(data_metadata_block);
        let metadata_block = MetadataBlock {
            inner: Arc::new(RwLock::new(inner_block)),
        };

        // Write metadata block into previous
        {
            let mut write_guard = self.previous.write().unwrap();
            *write_guard = metadata_block.clone();
        }

        // Send metadatablock to be flushed
        METADATA_BLOCK_WRITER.try_send(metadata_block).unwrap();

        // reset to zero
        self.len.store(0, SeqCst);
    }

    fn read(&self) -> ReadOnlyMetadataBlock {
        let len = self.len.load(SeqCst);
        let blocks: &[MetadataEntry] =
            unsafe { MaybeUninit::slice_assume_init_ref(&self.block[..len]) };

        let mut block_vec = Vec::new();
        for entry in blocks.iter() {
            let data_block = ReadOnlyDataBlock::from(&entry.data_block);
            let entry =
                ReadOnlyMetadataEntry::new(data_block, entry.time_range.min, entry.time_range.max);
            block_vec.push(entry);
        }
        let previous = self.previous.read().unwrap().clone();
        previous.read_only(block_vec)
    }
}

#[derive(Copy, Clone)]
struct TimeRange {
    min: u64,
    max: u64,
}

#[derive(Clone)]
pub struct MetadataBlock {
    inner: Arc<RwLock<InnerMetadataBlock>>,
}

impl MetadataBlock {
    fn flush(&self, partition: i32, producer: &mut Producer) {
        let read_guard = self.inner.read().unwrap();

        // Create an InnerMetadataBlock::Kafka
        let inner = match &*read_guard {
            InnerMetadataBlock::Kafka(_) => panic!("MetadataBlock is flushing twice!"),
            InnerMetadataBlock::Data(x) => {
                // The previous should have been flushed
                let (previous_kafka, previous_time_range) = match &*x.previous.inner.read().unwrap()
                {
                    InnerMetadataBlock::Kafka(k) => (k.block.clone(), k.time_range),
                    InnerMetadataBlock::Data(_) => {
                        panic!("The previous block has not been flushed!")
                    }
                };

                // Flush each item in the block
                for e in x.block.iter() {
                    e.flush(partition, producer)
                }

                // build ReadOnlyMetadataBlock
                let mut blocks = Vec::new();
                for entry in x.block.iter() {
                    let data_block = ReadOnlyDataBlock::from(&entry.data_block);
                    let entry = ReadOnlyMetadataEntry::new(
                        data_block,
                        entry.time_range.min,
                        entry.time_range.max,
                    );
                    blocks.push(entry);
                }

                let readonly = ReadOnlyMetadataBlock::new(
                    blocks,
                    previous_kafka,
                    previous_time_range.min,
                    previous_time_range.max,
                );
                let bytes = bincode::serialize(&readonly).unwrap();
                let entry = producer.send(partition, bytes.as_slice());
                info!("Kafka flush: {:?}", entry);

                InnerMetadataBlock::Kafka(KafkaMetadataBlock {
                    block: entry,
                    time_range: x.time_range,
                })
            }
        };
        drop(read_guard);

        // Implicitly, this drops the previous kafka metadata block in the list (along with the
        // data that was in data block);
        *self.inner.write().unwrap() = inner;
    }

    fn read_only(&self, mut blocks: Vec<ReadOnlyMetadataEntry>) -> ReadOnlyMetadataBlock {
        let read_guard = self.inner.read().unwrap();
        match &*read_guard {
            // Reached the end of the list. Create a new readonly metadata block
            InnerMetadataBlock::Kafka(x) => {
                info!("Metadata Block will be loaded from kafka");
                blocks.sort_by_key(|x| (x.min, x.max));
                ReadOnlyMetadataBlock::new(
                    blocks,
                    x.block.clone(),
                    x.time_range.min,
                    x.time_range.max,
                )
            }

            // Somewhere in the middle of the list, take all the entries and push into the vector,
            // then continue traversing to next metadata block via recursion
            // This will terminate because the first Metadata block was initialized as a kafka
            // entry
            InnerMetadataBlock::Data(x) => {
                info!("Metadata Block being read from memory");
                for entry in x.block.iter() {
                    let data_block = ReadOnlyDataBlock::from(&entry.data_block);
                    let entry = ReadOnlyMetadataEntry::new(
                        data_block,
                        entry.time_range.min,
                        entry.time_range.max,
                    );
                    blocks.push(entry);
                }
                let previous_cloned = x.previous.clone();
                drop(read_guard); // need to drop guard here because traversal would lock out all accessors
                previous_cloned.read_only(blocks)
            }
        }
    }
}

struct KafkaMetadataBlock {
    block: KafkaEntry, // points to a ReadOnlyMetadataBlock
    time_range: TimeRange,
}

struct DataMetadataBlock {
    block: [MetadataEntry; METADATA_BLOCK_SZ],
    time_range: TimeRange,
    previous: Arc<MetadataBlock>,
}

enum InnerMetadataBlock {
    Kafka(KafkaMetadataBlock),
    Data(DataMetadataBlock),
}

#[derive(Clone)]
pub struct MetadataEntry {
    data_block: DataBlock,
    time_range: TimeRange,
}

impl MetadataEntry {
    fn flush(&self, partition: i32, producer: &mut Producer) {
        self.data_block.flush(partition, producer);
    }
}
