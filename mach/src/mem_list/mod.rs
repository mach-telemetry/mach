mod source_block_list;
pub use source_block_list::{
    SourceBlockList, SourceBlocks2, SourceBlocks3,
};

use crate::constants::*;
use crate::{
    compression::Compression,
    id::SeriesId,
    segment::FlushSegment,
    snapshot::Segment,
    utils::byte_buffer::ByteBuffer,
    utils::kafka,
    utils::timer::*,
    utils::wp_lock::{NoDealloc, WpLock},
};
use dashmap::DashMap;
use lazy_static::lazy_static;
use serde::*;
use std::cell::RefCell;
use std::collections::HashMap;
use std::convert::TryInto;
use std::mem;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering::SeqCst},
    Arc, RwLock,
};
use std::thread;
use std::time::Duration;
use lzzzz::lz4;

#[allow(dead_code)]
static QUEUE_LEN: AtomicUsize = AtomicUsize::new(0);

#[allow(dead_code)]
static BLOCK_LIST_ENTRY_ID: AtomicUsize = AtomicUsize::new(0);

static CHUNK_ID: AtomicUsize = AtomicUsize::new(0);

lazy_static! {
    //pub static ref BLOCKLIST_ENTRIES: Arc<AtomicUsize> = {
    //    let x = Arc::new(AtomicUsize::new(0));
    //    let x2 = x.clone();
    //    thread::spawn(move || loop {
    //        let x = x2.load(SeqCst);
    //        println!("Block Entries {}", x);
    //        thread::sleep(Duration::from_secs(1));
    //    });
    //    x
    //};
    pub static ref PENDING_UNFLUSHED_BYTES: Arc<AtomicUsize> = {
        let x = Arc::new(AtomicUsize::new(0));
        let x2 = x.clone();
        thread::spawn(move || loop {
            let x = x2.load(SeqCst);
            println!("Pending unflushed data bytes: {}", x);
            thread::sleep(Duration::from_secs(1));
        });
        x
    };
    pub static ref PENDING_UNFLUSHED_BLOCKS: Arc<AtomicUsize> = {
        let x = Arc::new(AtomicUsize::new(0));
        let x2 = x.clone();
        thread::spawn(move || loop {
            let x = x2.load(SeqCst);
            println!("Pending unflushed data blocks: {}", x);
            thread::sleep(Duration::from_secs(1));
        });
        x
    };

    //static ref TOPIC: String = random_id();
    pub static ref TOTAL_BYTES_FLUSHED: Arc<AtomicUsize> = {
        let x = Arc::new(AtomicUsize::new(0));
        let x2 = x.clone();
        thread::spawn(move || loop {
            let x = x2.load(SeqCst);
            println!("Total data bytes flushed: {}", x);
            thread::sleep(Duration::from_secs(1));
        });
        x
    };
    pub static ref COMPRESSED_BYTES: Arc<AtomicUsize> = {
        let x = Arc::new(AtomicUsize::new(0));
        let x2 = x.clone();
        thread::spawn(move || loop {
            let x = x2.load(SeqCst);
            println!("Total compressed bytes: {}", x);
            thread::sleep(Duration::from_secs(1));
        });
        x
    };
    static ref N_FLUSHERS: AtomicUsize = AtomicUsize::new(INIT_FLUSHERS);

    static ref FLUSH_WORKERS: crossbeam::channel::Sender<Arc<BlockListEntry>> = {
        //let flushers = DashMap::new();
        let (tx, rx) = crossbeam::channel::unbounded();
        for idx in 0..N_FLUSHERS.load(SeqCst) {
            let rx = rx.clone();
            std::thread::Builder::new().name(format!("Kafka Flusher {}", idx)).spawn(move || {
                flush_worker(idx, rx);
            }).unwrap();
            //flushers.insert(idx, tx);
        }
        tx
        //flushers
    };
}

#[derive(Debug, Copy, Clone)]
pub enum Error {
    Snapshot,
}

//pub(self) fn add_flush_worker() {
//    let idx = N_FLUSHERS.fetch_add(1, SeqCst);
//    let (tx, rx) = crossbeam::channel::unbounded();
//    std::thread::spawn(move || {
//        flush_worker(rx);
//    });
//    FLUSH_WORKERS.insert(idx, tx);
//    println!("ADDED FLUSH WORKER: {}", idx);
//}

fn flush_worker(id: usize, chan: crossbeam::channel::Receiver<Arc<BlockListEntry>>) {
    let mut producer = kafka::Producer::new();
    while let Ok(block) = chan.recv() {
        println!("Flushing at flush worker: {}", id);
        let block_len = block.len;
        block.flush(id as i32 % PARTITIONS, &mut producer);
        PENDING_UNFLUSHED_BYTES.fetch_sub(block_len, SeqCst);
        PENDING_UNFLUSHED_BLOCKS.fetch_sub(1, SeqCst);
        TOTAL_BYTES_FLUSHED.fetch_add(block_len, SeqCst);
    }
    println!("FLUSHER EXITING");
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
    id_set: RefCell<HashMap<SeriesId, (u64, u64)>>,
    series_map: Arc<DashMap<SeriesId, Arc<SourceBlockList>>>,
    //chunks: Arc<DashMap<usize, Segment>>,
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
            id_set: RefCell::new(HashMap::new()),
            series_map: Arc::new(DashMap::new()),
            //chunks: Arc::new(DashMap::new()),
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
        let list = Arc::new(SourceBlockList::new(series_id));
        self.series_map.insert(series_id, list.clone());
        list
    }

    pub fn push(&self, series_id: SeriesId, segment: &FlushSegment, compression: &Compression) {
        // Safety: id() is atomic
        //let block_id = unsafe { self.head_block.unprotected_read().id.load(SeqCst) };

        //let count = *self.counter.borrow() + 1;
        //self.chunks.insert(count, segment.to_flush().unwrap().read());
        //*self.counter.borrow_mut() += 1;

        let time_range = {
            let x = segment.to_flush().unwrap();
            let t = x.timestamps();
            (t[0], t[t.len() - 1])
        };

        // Safety: unprotected write because Block push only appends data and increments an atomic
        // read is bounded by the atomic
        let is_full = unsafe {
            self.head_block
                .unprotected_write()
                .push(series_id, segment, compression)
        };
        {
            let mut borrowed_map = self.id_set.borrow_mut();
            let e = borrowed_map
                .entry(series_id)
                .or_insert((u64::MAX, u64::MAX));
            if e.0 == u64::MAX {
                *e = time_range;
            } else {
                assert!(e.1 < time_range.1);
                e.1 = time_range.1;
            }
        }

        if is_full {
            // Safety: there should be only one writer and it should be doing this push
            let block = unsafe { self.head_block.unprotected_read() };
            //let num_bytes = block.len.load(SeqCst);
            let copy: Arc<BlockListEntry> = Arc::new(block.as_read_only());

            for (id, time_range) in self.id_set.borrow_mut().drain() {
                let min_ts = time_range.0;
                let max_ts = time_range.1;
                assert!(min_ts < max_ts);
                self.series_map
                    .get(&id)
                    .unwrap()
                    .push(min_ts, max_ts, copy.clone());
            }
            PENDING_UNFLUSHED_BYTES.fetch_add(copy.len, SeqCst);
            PENDING_UNFLUSHED_BLOCKS.fetch_add(1, SeqCst);
            //let n_flushers = N_FLUSHERS.load(SeqCst);
            FLUSH_WORKERS.send(copy).unwrap();
            //// Mark current block as cleared
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
    offsets: Box<[(u64, usize)]>,
}

impl Block {
    fn new() -> Self {
        Self {
            id: AtomicU64::new(0),
            bytes: vec![0u8; BLOCK_SZ * 2].into_boxed_slice(),
            len: AtomicUsize::new(std::mem::size_of::<u64>()), // id starts at 0
            items: AtomicUsize::new(0),
            offsets: vec![(0, 0); BLOCK_SZ].into_boxed_slice(),
        }
    }

    fn push(
        &mut self,
        series_id: SeriesId,
        segment: &FlushSegment,
        compression: &Compression,
    ) -> bool {
        let chunk_id = CHUNK_ID.fetch_add(1, SeqCst);
        let start_offset = self.len.load(SeqCst);

        let bytes = &mut self.bytes[start_offset..];
        let items = self.items.load(SeqCst);
        //let u64sz = mem::size_of::<u64>();

        let min_ts = segment.to_flush().unwrap().timestamps()[0];
        let max_ts = *segment.to_flush().unwrap().timestamps().last().unwrap();

        bytes[0..8].copy_from_slice(&1234567890u64.to_be_bytes());
        bytes[8..16].copy_from_slice(&chunk_id.to_be_bytes());
        bytes[16..24].copy_from_slice(&series_id.0.to_be_bytes()); // series ID
        bytes[24..32].copy_from_slice(&min_ts.to_be_bytes()); // series ID
        bytes[32..40].copy_from_slice(&max_ts.to_be_bytes()); // series ID
        bytes[40..48].copy_from_slice(&0u64.to_be_bytes()); // place holder for chunk size

        // Compress the data into the buffer
        let size = {
            let mut byte_buffer = ByteBuffer::new(&mut bytes[48..]);
            assert!(byte_buffer.len() == 0);
            compression.compress(&segment.to_flush().unwrap(), &mut byte_buffer);
            byte_buffer.len()
        };
        //COMPRESSED_BYTES.fetch_add(size, SeqCst);

        // Write the chunk size
        bytes[40..48].copy_from_slice(&size.to_be_bytes());

        // store location of this thing
        //if series_id.0 == 4560055620737106128 {
        //    println!("start offset: {}", start_offset);
        //    println!("magic: {:?}", &bytes[24..24+12]);
        //}
        self.offsets[items] = (series_id.0, start_offset);

        //calculate new length
        let new_length = start_offset + 48 + size;

        // update length
        self.len.store(new_length, SeqCst);
        self.items.store(items + 1, SeqCst);

        // return true if full
        new_length > BLOCK_SZ
    }

    fn reset(&mut self) {
        //println!("resetting");
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

        let mut v = Vec::new();
        v.extend_from_slice(&bytes.len().to_be_bytes());
        lz4::compress_to_vec(bytes.as_slice(), &mut v, 1).unwrap();
        let compressed_len = v.len();
        println!("BlockListEntry compressed {} -> {}", len, compressed_len);

        let inner = InnerBlockListEntry::new_bytes(v.into());
        BlockListEntry {
            inner: RwLock::new(inner),
            len: compressed_len,
            _id: BLOCK_LIST_ENTRY_ID.fetch_add(1, SeqCst),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ChunkBytes {
    pub id: usize,
    pub min_ts: u64,
    pub max_ts: u64,
    pub data: Box<[u8]>,
}

pub struct ReadOnlyBlockBytes(Arc<[u8]>);

impl ReadOnlyBlockBytes {

    pub fn new_from_compressed(data: &[u8]) -> Self {
        let sz = usize::from_be_bytes(data[..8].try_into().unwrap());
        let mut vec = vec![0u8; sz];
        lz4::decompress(&data[8..], &mut vec).unwrap();
        Self(vec.into())
    }

    pub fn id(&self) -> usize {
        usize::from_be_bytes(self.0[..8].try_into().unwrap())
    }

    fn data_len_idx(&self) -> usize {
        self.0.len() - 8
    }

    fn data_len(&self) -> usize {
        usize::from_be_bytes(self.0[self.data_len_idx()..].try_into().unwrap())
    }

    pub fn offsets(&self) -> Box<[(u64, usize)]> {
        let data_len = self.data_len();
        let segments: Box<[(u64, usize)]> =
            bincode::deserialize(&self.0[data_len..self.data_len_idx()]).unwrap();
        //let mut no_id = true;
        //for (id, _) in segments.iter() {
        //    //if *id == 4560055620737106128 {
        //    //    no_id = false;
        //    //}
        //}
        //if no_id {
        //    println!("NO ID");
        //}
        //println!("{:?}", segments);
        segments
    }

    pub fn chunks_for_id(&self, id: u64) -> Vec<ChunkBytes> {
        let offsets = self.offsets();
        let mut result = Vec::new();
        for (i, offset) in offsets.iter() {
            if *i == id {
                let bytes = &self.0[*offset..];

                let magic = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
                let chunk_id = usize::from_be_bytes(bytes[8..16].try_into().unwrap());
                let _series_id = u64::from_be_bytes(bytes[16..24].try_into().unwrap());
                let min_ts = u64::from_be_bytes(bytes[24..32].try_into().unwrap());
                let max_ts = u64::from_be_bytes(bytes[32..40].try_into().unwrap());
                let chunk_size = u64::from_be_bytes(bytes[40..48].try_into().unwrap()) as usize;

                assert_eq!(magic, 1234567890);
                //println!("{} {}",series_id, chunk_size);

                // decompress data
                let bytes = &bytes[48..48 + chunk_size];
                let entry = ChunkBytes {
                    id: chunk_id,
                    min_ts,
                    max_ts,
                    data: bytes.into(),
                };
                result.push(entry);
            }
        }
        result
    }

    pub fn segment_at_offset(&self, offset: usize, segment: &mut Segment) {
        //println!("getting segment at offset: {}", offset);

        let bytes = &self.0[offset..];
        let magic = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
        //let chunk_id = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
        let _series_id = u64::from_be_bytes(bytes[16..24].try_into().unwrap()) as usize;
        let chunk_size = u64::from_be_bytes(bytes[24..32].try_into().unwrap()) as usize;

        assert_eq!(magic, 1234567890);
        //println!("{} {}",series_id, chunk_size);

        // decompress data
        let bytes = &bytes[32..32 + chunk_size];

        Compression::decompress(bytes, segment).unwrap();
        //println!("size decompressed: {}", size);
    }
}

impl std::ops::Deref for ReadOnlyBlockBytes {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub enum ChunkBytesOrKafka {
    Bytes(Vec<ChunkBytes>),
    Kafka(kafka::KafkaEntry),
}

impl ChunkBytesOrKafka {
    pub fn to_bytes(&self, id: u64) -> Self {
        match self {
            Self::Bytes(_) => self.clone(),
            Self::Kafka(entry) => Self::Bytes(
                ReadOnlyBlock::Offset(entry.clone())
                    .as_bytes()
                    .chunks_for_id(id),
            ),
        }
    }

    pub fn segment_at_offset(&self, offset: usize, segment: &mut Segment) {
        match self {
            Self::Bytes(x) => {
                let chunk_bytes = &x[offset];
                Compression::decompress(&chunk_bytes.data[..], segment).unwrap();
            }
            _ => panic!("Need to make bytes first!"),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Bytes(x) => x.len(),
            _ => panic!("Need to make bytes first!"),
        }
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub enum ReadOnlyBlock {
    Bytes(Box<[u8]>),
    Offset(kafka::KafkaEntry),
}

impl std::convert::From<InnerBlockListEntry> for ReadOnlyBlock {
    fn from(item: InnerBlockListEntry) -> Self {
        match &item {
            InnerBlockListEntry::Bytes(x) => ReadOnlyBlock::Bytes(x[..].into()),
            InnerBlockListEntry::Offset(entry) => ReadOnlyBlock::Offset(entry.clone()),
        }
    }
}

impl ReadOnlyBlock {
    pub fn chunk_bytes_or_kafka(&self, id: u64) -> ChunkBytesOrKafka {
        match self {
            Self::Bytes(x) => {
                ChunkBytesOrKafka::Bytes(ReadOnlyBlockBytes::new_from_compressed(&x[..]).chunks_for_id(id))
            }
            Self::Offset(x) => ChunkBytesOrKafka::Kafka(x.clone()),
        }
    }

    pub fn as_bytes(&self) -> ReadOnlyBlockBytes {
        let _timer = ThreadLocalTimer::new("ReadOnlyBlock::as_bytes");
        match self {
            ReadOnlyBlock::Bytes(x) => ReadOnlyBlockBytes::new_from_compressed(&x[..]),
            ReadOnlyBlock::Offset(entry) => {
                let mut vec = Vec::new();
                entry.load(&mut vec).unwrap();
                ReadOnlyBlockBytes::new_from_compressed(&vec)
            }
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct ReadOnlyBlock2 {
    pub min_ts: u64,
    pub max_ts: u64,
    pub block: ReadOnlyBlock,
}

impl std::ops::Deref for ReadOnlyBlock2 {
    type Target = ReadOnlyBlock;
    fn deref(&self) -> &Self::Target {
        &self.block
    }
}

impl ReadOnlyBlock2 {
    pub fn to_readonlyblock_3(&self, id: u64) -> ReadOnlyBlock3 {
        let block = self.block.chunk_bytes_or_kafka(id);
        ReadOnlyBlock3 {
            id,
            min_ts: self.min_ts,
            max_ts: self.max_ts,
            block,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct ReadOnlyBlock3 {
    pub id: u64,
    pub min_ts: u64,
    pub max_ts: u64,
    pub block: ChunkBytesOrKafka,
}

impl ReadOnlyBlock3 {
    //pub fn to_bytes(&mut self) {
    //    self.block = self.block.to_bytes(self.id);
    //}

    //pub fn chunk_bytes(&mut self) -> &[ChunkBytes] {
    //    self.to_bytes();
    //    match &self.block {
    //        ChunkBytesOrKafka::Bytes(x) => x.as_slice(),
    //        _ => unreachable!(),
    //    }
    //}

    //pub fn segment_at_offset(&mut self, offset: usize, segment: &mut Segment) {
    //    let bytes = &self.chunk_bytes()[offset];
    //    Compression::decompress(&bytes.data[..], segment).unwrap();
    //}
}

#[derive(Serialize, Deserialize, Clone)]
pub struct KafkaMetadata {
    pub min: u64,
    pub max: u64,
    pub entry: kafka::KafkaEntry,
}

#[derive(Clone)]
enum InnerBlockListEntry {
    Bytes(Arc<[u8]>),
    Offset(kafka::KafkaEntry),
}

impl InnerBlockListEntry {
    fn new_bytes(data: Arc<[u8]>) -> Self {
        //BLOCKLIST_ENTRIES.fetch_add(1, SeqCst);
        InnerBlockListEntry::Bytes(data)
    }
}

//impl Drop for InnerBlockListEntry {
//    fn drop(&mut self) {
//        match self {
//            Self::Bytes(_) => {BLOCKLIST_ENTRIES.fetch_sub(1, SeqCst);},
//            _ => {},
//        };
//    }
//}

pub struct BlockListEntry {
    inner: RwLock<InnerBlockListEntry>,
    len: usize,
    _id: usize,
}

impl BlockListEntry {
    fn set_partition_offset(&self, entry: kafka::KafkaEntry) {
        let mut guard = self.inner.write().unwrap();
        match &mut *guard {
            InnerBlockListEntry::Bytes(_x) => {
                let _x = std::mem::replace(&mut *guard, InnerBlockListEntry::Offset(entry));
            }
            InnerBlockListEntry::Offset(..) => {
                println!("Double flushed");
            }
        }
    }

    fn inner(&self) -> InnerBlockListEntry {
        self.inner.read().unwrap().clone()
    }

    fn flush(&self, partition: i32, producer: &mut kafka::Producer) {
        let guard = self.inner.read().unwrap();
        match &*guard {
            InnerBlockListEntry::Bytes(bytes) => {
                //let num_bytes = bytes.len();
                let kafka_entry = producer.send(partition, bytes);
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
