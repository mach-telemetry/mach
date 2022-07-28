mod source_block_list;
pub use source_block_list::{SourceBlockList, SourceBlocks, UNFLUSHED_COUNT};

use crate::{
    compression::Compression,
    id::SeriesId,
    segment::FlushSegment,
    snapshot::Segment,
    utils::byte_buffer::ByteBuffer,
    utils::kafka::{self, BOOTSTRAPS, TOPIC},
    utils::wp_lock::{NoDealloc, WpLock},
};
use dashmap::DashMap;
use lazy_static::lazy_static;
use rand::{thread_rng, Rng};
use std::cell::RefCell;
use std::collections::HashSet;
use std::convert::TryInto;
use std::mem;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering::SeqCst},
    Arc, RwLock,
};


#[allow(dead_code)]
static QUEUE_LEN: AtomicUsize = AtomicUsize::new(0);

pub const INIT_FLUSHERS: usize = 1;
pub const BLOCK_SZ: usize = 1_000_000;

lazy_static! {
    //static ref TOPIC: String = random_id();
    pub static ref TOTAL_BYTES_FLUSHED: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    static ref N_FLUSHERS: AtomicUsize = AtomicUsize::new(INIT_FLUSHERS);
    static ref FLUSH_WORKERS: DashMap<usize, crossbeam::channel::Sender<Arc<BlockListEntry>>> = {
        let flushers = DashMap::new();
        for idx in 0..N_FLUSHERS.load(SeqCst) {
            let (tx, rx) = crossbeam::channel::unbounded();
            std::thread::spawn(move || {
                flush_worker(rx);
            });
            flushers.insert(idx, tx);
        }
        flushers
    };
}

#[derive(Debug, Copy, Clone)]
pub enum Error {
    Snapshot,
}

pub(self) fn add_flush_worker() {
    let idx = N_FLUSHERS.fetch_add(1, SeqCst);
    let (tx, rx) = crossbeam::channel::unbounded();
    std::thread::spawn(move || {
        flush_worker(rx);
    });
    FLUSH_WORKERS.insert(idx, tx);
    println!("ADDED FLUSH WORKER: {}", idx);
}

fn flush_worker(chan: crossbeam::channel::Receiver<Arc<BlockListEntry>>) {
    let mut producer = kafka::Producer::new();
    while let Ok(block) = chan.recv() {
        block.flush(&mut producer);
    }
}

//struct List {
//    _id: SeriesId,
//    head: AtomicU64,
//}
//
//impl List {
//    pub fn new(id: SeriesId) -> Self {
//        List {
//            _id,
//            head: AtomicU64::new(u64::MAX)
//        }
//    }
//}

pub struct BlockList {
    head_block: WpLock<Block>,
    id_set: RefCell<HashSet<SeriesId>>,
    series_map: Arc<DashMap<SeriesId, Arc<SourceBlockList>>>,
}

/// Safety
/// Blocklist isn't sync/send because of id_set w/c is RefCell but this is only used in one thread
unsafe impl Sync for BlockList {}
unsafe impl Send for BlockList {}

impl BlockList {
    pub fn new() -> Self {
        kafka::make_topic(BOOTSTRAPS, TOPIC);
        BlockList {
            head_block: WpLock::new(Block::new()),
            id_set: RefCell::new(HashSet::new()),
            series_map: Arc::new(DashMap::new()),
        }
    }

    // Meant to be called from a separate reader thread. "as_read_only" makes an InnerBlockEntry
    // (copies). head_block.inner().into() locks the InnerBlockEntry and converts to a
    // ReadOnlyBlock - no contention here.
    pub fn snapshot(&self) -> Result<ReadOnlyBlock, Error> {
        // Protected because the block could be reset while making the copy
        let guard = self.head_block.protected_read();

        let head_block = guard.as_read_only();
        if guard.release().is_err() {
            Err(Error::Snapshot)
        } else {
            Ok(head_block.inner().into())
        }
    }

    pub fn add_source(&self, series_id: SeriesId) -> Arc<SourceBlockList> {
        let list = Arc::new(SourceBlockList::new());
        self.series_map.insert(series_id, list.clone());
        list
    }

    pub fn push(&self, series_id: SeriesId, segment: &FlushSegment, compression: &Compression) {
        // Safety: id() is atomic
        //let block_id = unsafe { self.head_block.unprotected_read().id.load(SeqCst) };

        // Safety: unprotected write because Block push only appends data and increments an atomic
        // read is bounded by the atomic
        let is_full = unsafe {
            self.head_block
                .unprotected_write()
                .push(series_id, segment, compression)
        };

        self.id_set.borrow_mut().insert(series_id);

        if is_full {
            // Safety: there should be only one writer and it should be doing this push
            let copy = Arc::new(unsafe { self.head_block.unprotected_read().as_read_only() });
            for id in self.id_set.borrow_mut().drain() {
                self.series_map.get(&id).unwrap().push(copy.clone());
            }
            let n_flushers = N_FLUSHERS.load(SeqCst);
            FLUSH_WORKERS
                .get(&thread_rng().gen_range(0..n_flushers))
                .unwrap()
                .value()
                .send(copy)
                .unwrap();
            // Mark current block as cleared
            self.head_block.protected_write().reset();
        }
    }
}

/// Safety:
/// for internal use only. see use in Block struct
unsafe impl NoDealloc for Block {}

struct Block {
    id: AtomicU64,
    bytes: Box<[u8]>,
    len: AtomicUsize,
    items: AtomicUsize,
    offsets: [(u64, usize); 1_000],
}

impl Block {
    fn new() -> Self {
        Self {
            id: AtomicU64::new(0),
            bytes: vec![0u8; BLOCK_SZ * 2].into_boxed_slice(),
            len: AtomicUsize::new(std::mem::size_of::<u64>()), // id starts at 0
            items: AtomicUsize::new(0),
            offsets: [(0, 0); 1_000],
        }
    }

    fn push(
        &mut self,
        series_id: SeriesId,
        segment: &FlushSegment,
        compression: &Compression,
    ) -> bool {
        let mut offset = self.len.load(SeqCst);
        let start_offset = offset;
        let items = self.items.load(SeqCst);
        let u64sz = mem::size_of::<u64>();

        // Write SeriesId
        let end = offset + u64sz;
        self.bytes[offset..end].copy_from_slice(&series_id.0.to_be_bytes());
        offset = end;

        // Reserve space for the size of the chunk
        let end = offset + u64sz;
        self.bytes[offset..end].copy_from_slice(&0u64.to_be_bytes());
        let size_offset = offset;
        offset = end;

        // Compress the data into the buffer
        offset += {
            let mut byte_buffer = ByteBuffer::new(&mut self.bytes[offset..]);
            compression.compress(&segment.to_flush().unwrap(), &mut byte_buffer);
            byte_buffer.len()
        };

        // Write the chunk
        let size = (offset - size_offset) as u64;
        self.bytes[size_offset..size_offset + u64sz].copy_from_slice(&size.to_be_bytes());

        // Write the seriesID and offset
        self.offsets[items] = (series_id.0, start_offset);

        // update length
        self.items.store(items + 1, SeqCst);
        self.len.store(offset, SeqCst);

        // return true if full
        offset > BLOCK_SZ
    }

    fn reset(&mut self) {
        let u64sz = mem::size_of::<u64>();
        let new_id = self.id.fetch_add(1, SeqCst) + 1;
        self.bytes[..u64sz].copy_from_slice(&new_id.to_be_bytes());
        self.items.store(0, SeqCst);
        self.len.store(u64sz, SeqCst);
    }

    fn as_read_only(&self) -> BlockListEntry {
        let len = self.len.load(SeqCst);
        let item_count = self.items.load(SeqCst);
        let mut bytes: Vec<u8> = self.bytes[..len].into();
        bincode::serialize_into(&mut bytes, &self.offsets[..item_count]).unwrap();
        bytes.extend_from_slice(&len.to_be_bytes());
        BlockListEntry {
            inner: RwLock::new(InnerBlockListEntry::Bytes(bytes.into())),
        }
    }
}

pub struct ReadOnlyBlockBytes(Arc<[u8]>);

impl ReadOnlyBlockBytes {
    fn data_len_idx(&self) -> usize {
        self.0.len() - 8
    }

    fn data_len(&self) -> usize {
        usize::from_be_bytes(self.0[self.data_len_idx()..].try_into().unwrap())
    }

    pub fn offsets(&self) -> Box<[(u64, usize)]> {
        let data_len = self.data_len();
        bincode::deserialize(&self.0[data_len..self.data_len_idx()]).unwrap()
    }

    pub fn segment_at_offset(&self, offset: usize, segment: &mut Segment) {
        let bytes = &self.0[offset..];

        // Parse data per Block::push()
        // series ID
        let _series_id = SeriesId(u64::from_be_bytes(bytes[..8].try_into().unwrap()));

        // size of data bytes
        let size = usize::from_be_bytes(bytes[8..16].try_into().unwrap());

        // decompress data
        let bytes = &bytes[16..16 + size];
        let _ = Compression::decompress(bytes, segment).unwrap();
    }
}

impl std::ops::Deref for ReadOnlyBlockBytes {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub enum ReadOnlyBlock {
    Bytes(Box<[u8]>),
    Offset(kafka::KafkaEntry),
}

impl std::convert::From<InnerBlockListEntry> for ReadOnlyBlock {
    fn from(item: InnerBlockListEntry) -> Self {
        match item {
            InnerBlockListEntry::Bytes(x) => ReadOnlyBlock::Bytes(x[..].into()),
            InnerBlockListEntry::Offset(entry) => ReadOnlyBlock::Offset(entry),
        }
    }
}

impl ReadOnlyBlock {
    pub fn as_bytes(&self) -> ReadOnlyBlockBytes {
        match self {
            ReadOnlyBlock::Bytes(x) => ReadOnlyBlockBytes(x.clone().into()),
            ReadOnlyBlock::Offset(entry) => {
                let mut vec = Vec::new();
                entry.load(&mut vec).unwrap();
                ReadOnlyBlockBytes(vec.into())
            }
        }
    }
}

#[derive(Clone)]
enum InnerBlockListEntry {
    Bytes(Arc<[u8]>),
    Offset(kafka::KafkaEntry),
}

pub struct BlockListEntry {
    inner: RwLock<InnerBlockListEntry>,
}

impl BlockListEntry {
    fn set_partition_offset(&self, entry: kafka::KafkaEntry) {
        let mut guard = self.inner.write().unwrap();
        match &mut *guard {
            InnerBlockListEntry::Bytes(_x) => {
                let _x = std::mem::replace(&mut *guard, InnerBlockListEntry::Offset(entry));
            }
            InnerBlockListEntry::Offset(..) => {}
        }
    }

    fn inner(&self) -> InnerBlockListEntry {
        self.inner.read().unwrap().clone()
    }

    fn flush(&self, producer: &mut kafka::Producer) {
        let guard = self.inner.read().unwrap();
        match &*guard {
            InnerBlockListEntry::Bytes(bytes) => {
                let kafka_entry = producer.send(bytes);
                TOTAL_BYTES_FLUSHED.fetch_add(bytes.len(), SeqCst);
                drop(guard);
                self.set_partition_offset(kafka_entry);
            }
            InnerBlockListEntry::Offset(_) => {} // already flushed
        }
    }

    fn partition_offset(&self) -> kafka::KafkaEntry {
        match &*self.inner.read().unwrap() {
            InnerBlockListEntry::Bytes(_) => unimplemented!(),
            InnerBlockListEntry::Offset(entry) => entry.clone(),
        }
    }
}

//pub struct ReadOnlySegment {
//    id: SeriesId,
//    next_block: usize,
//    start: usize,
//    end: usize,
//    ro_bytes: ReadOnlyBytes,
//}
//
//#[derive(Clone)]
//pub struct ReadOnlyBytes(Arc<[u8]>);
//
//impl ReadOnlyBytes {
//    fn source_id(&self) -> usize {
//        usize::from_be_bytes(self.0[..8].try_into().unwrap())
//    }
//
//    pub fn get_segments(&self) -> Vec<ReadOnlySegment> {
//        let mut v = Vec::new();
//        let mut offset = 8;
//        while offset < self.0.len() {
//            let id = {
//                let id = u64::from_be_bytes(self.0[offset..offset+8].try_into().unwrap());
//                offset += 8;
//                SeriesId(id)
//            };
//
//            let next_block = usize::from_be_bytes(self.0[offset..offset+8].try_into().unwrap());
//            offset += 8;
//
//            let segment_sz = usize::from_be_bytes(self.0[offset..offset+8].try_into().unwrap());
//            offset += 8;
//
//            let start = offset;
//            let end = offset + segment_sz;
//
//            v.push(ReadOnlySegment {
//                id,
//                next_block,
//                start,
//                end,
//                ro_bytes: self.clone(),
//            });
//        }
//        v
//    }
//}
