mod circular_list;

use circular_list::IndexList;

use crate::{
    compression::Compression,
    id::SeriesId,
    segment::FlushSegment,
    utils::wp_lock::{WpLock, NoDealloc},
    utils::byte_buffer::ByteBuffer,
    utils::random_id,
    utils::kafka,
};
use std::sync::{Arc, RwLock, Mutex, mpsc::{Sender, Receiver, channel}, atomic::{AtomicU64, AtomicUsize, Ordering::SeqCst}};
use std::mem;
use std::time::{Duration, SystemTime};
use std::cell::RefCell;
use std::collections::HashSet;
use dashmap::DashMap;
use lazy_static::lazy_static;
use rdkafka::{
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord},
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
};
use rand::{Rng, thread_rng};
use std::convert::TryInto;

const FLUSHERS: usize = 1;
static QUEUE_LEN: AtomicUsize = AtomicUsize::new(0);
const PARTITIONS: i32 = 3;
const REPLICAS: i32 = 3;
const BOOTSTRAPS: &str = "localhost:9093,localhost:9094,localhost:9095";

lazy_static! {
    static ref TOPIC: String = random_id();
    static ref FLUSH_WORKERS: Vec<crossbeam::channel::Sender<Arc<ReadOnlyBlock>>> = {
        let mut flushers = Vec::new();
        for _ in 0..FLUSHERS {
            let (tx, rx) = crossbeam::channel::unbounded();
            std::thread::spawn(move || {
                flush_worker(rx);
            });
            flushers.push(tx);
        }
        flushers
    };
}


fn make_topic(bootstrap_servers: &str, topic: &str) {
    println!("Creating topic");
    let client: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers.to_string())
        .create()
        .unwrap();
    let admin_opts = AdminOptions::new().request_timeout(Some(std::time::Duration::from_secs(5)));
    let topics = &[NewTopic {
        name: topic,
        num_partitions: PARTITIONS,
        replication: TopicReplication::Fixed(REPLICAS),
        config: Vec::new(),
    }];
    futures::executor::block_on(client.create_topics(topics, &admin_opts)).unwrap();
    println!("topic created: {}", TOPIC.as_str());
}

pub enum Error {
    Snapshot
}


fn flush_worker(chan: crossbeam::channel::Receiver<Arc<ReadOnlyBlock>>) {
    let mut producer = kafka::Producer::new(BOOTSTRAPS);
    while let Ok(block) = chan.recv() {
        let mut bytes = block.bytes();
        let part: i32= rand::thread_rng().gen_range(0..3);
        let (part2, offset) = producer.send(&*TOPIC, part, &bytes[..]);
        block.set_partition_offset(part.try_into().unwrap(), offset.try_into().unwrap());
    }
}

pub struct List {
    id: SeriesId,
    head: AtomicU64,
}

impl List {
    pub fn new(id: SeriesId) -> Self {
        List {
            id,
            head: AtomicU64::new(u64::MAX)
        }
    }
}

pub struct BlockList {
    head_block: WpLock<Block>,
    tail: AtomicU64,
    id_set: RefCell<HashSet<SeriesId>>,
    //block_map: DashMap<u64, (SystemTime, Arc<ReadOnlyBlock>)>,
    series_map: DashMap<SeriesId, IndexList>,
}

/// Safety
/// Blocklist isn't sync/send because of id_set w/c is RefCell but this is only used in one thread
unsafe impl Sync for BlockList {}
unsafe impl Send for BlockList {}

impl BlockList {
    pub fn new() -> Self {
        kafka::make_topic(BOOTSTRAPS, TOPIC.as_str());
        BlockList {
            head_block: WpLock::new(Block::new()),
            tail: AtomicU64::new(0),
            id_set: RefCell::new(HashSet::new()),
            series_map: DashMap::new(),
        }
    }

    pub fn snapshot(&self, series_id: SeriesId) -> Result<Vec<Arc<ReadOnlyBlock>>, Error> {
        let guard = self.head_block.protected_read();
        let head_block = guard.as_read_only();
        let head = guard.id.load(SeqCst);
        let tail = self.tail.load(SeqCst);
        if guard.release().is_err() {
            return Err(Error::Snapshot);
        };
        let mut snapshots = self.series_map.get(&series_id).unwrap().snapshot()?;
        let mut v = Vec::new();
        v.push(Arc::new(head_block));
        v.append(&mut snapshots);
        Ok(v)
    }

    pub fn push(
        &self,
        list: &List,
        series_id: SeriesId,
        segment: &FlushSegment,
        compression: &Compression,
    ) {

        // Safety: id() is atomic
        let block_id = unsafe { self.head_block.unprotected_read().id.load(SeqCst) };
        let head = list.head.load(SeqCst);

        // Safety: unprotected write because Block push only appends data and increments an atomic
        // read is bounded by the atomic
        let is_full = unsafe {
            self.head_block.unprotected_write().push(head, series_id, segment, compression)
        };

        // Update the list with the new head
        if head != block_id {
            list.head.store(block_id, SeqCst);
        }

        self.id_set.borrow_mut().insert(series_id);

        if is_full {
            // Safety: there should be only one writer and it should be doing this push
            let copy = Arc::new(unsafe { self.head_block.unprotected_read().as_read_only() });
            for id in self.id_set.borrow_mut().drain() {
                self.series_map.get(&id).unwrap().push(copy.clone());
            }
            FLUSH_WORKERS[thread_rng().gen_range(0..FLUSHERS)].send(copy).unwrap();
            // Mark current block as cleared
            self.head_block.protected_write().reset();
        }
    }
}

struct Bytes(Box<[u8]>);

impl std::ops::Deref for Bytes {
    type Target = Box<[u8]>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Bytes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Safety:
/// for internal use only. see use in Block struct
unsafe impl NoDealloc for Block{}

struct Block {
    id: AtomicU64,
    bytes: Bytes,
    len: AtomicUsize,
}

impl Block {
    fn new() -> Self {
        Self {
            id: AtomicU64::new(0),
            bytes: Bytes(vec![0u8; 2_000_000].into_boxed_slice()),
            len: AtomicUsize::new(std::mem::size_of::<u64>()), // id starts at 0
        }
    }

    fn push(
        &mut self,
        last_block_id: u64,
        series_id: SeriesId,
        segment: &FlushSegment,
        compression: &Compression,
    ) -> bool {
        let mut offset = self.len.load(SeqCst);
        let u64sz = mem::size_of::<u64>();

        // Write SeriesId
        let end = offset + u64sz;
        self.bytes[offset..end].copy_from_slice(&series_id.0.to_be_bytes());
        offset = end;

        // Write where we can find the next chunk for this SeriesId
        let end = offset + u64sz;
        self.bytes[offset..end].copy_from_slice(&last_block_id.to_be_bytes());
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

        // update length
        self.len.store(offset, SeqCst);

        // return true if full
        offset > 1_000_000
    }

    fn reset(&mut self) {
        let u64sz = mem::size_of::<u64>();
        let new_id = self.id.fetch_add(1, SeqCst) + 1;
        self.bytes[..u64sz].copy_from_slice(&new_id.to_be_bytes());
        self.len.store(u64sz, SeqCst);
    }

    fn as_read_only(&self) -> ReadOnlyBlock {
        let len = self.len.load(SeqCst);
        let bytes = self.bytes[..len].into();
        ReadOnlyBlock {
            inner: RwLock::new(InnerReadOnlyBlock::Bytes(bytes)),
        }
    }
}

enum InnerReadOnlyBlock {
    Bytes(Arc<[u8]>),
    Offset(usize, usize),
}

pub struct ReadOnlyBlock {
    inner: RwLock<InnerReadOnlyBlock>,
}

impl ReadOnlyBlock {

    fn set_partition_offset(&self, part: usize, off: usize) {
        let mut guard = self.inner.write().unwrap();
        let _x = std::mem::replace(&mut *guard, InnerReadOnlyBlock::Offset(part, off));
    }

    pub fn bytes(&self) -> Arc<[u8]> {
        match &*self.inner.read().unwrap() {
            InnerReadOnlyBlock::Bytes(x) => x.clone(),
            InnerReadOnlyBlock::Offset(x, y) => unimplemented!(),
        }
    }
}

pub struct ReadOnlySegment {
    id: SeriesId,
    next_block: usize,
    start: usize,
    end: usize,
    ro_bytes: ReadOnlyBytes,
}

#[derive(Clone)]
pub struct ReadOnlyBytes(Arc<[u8]>);

impl ReadOnlyBytes {
    fn source_id(&self) -> usize {
        usize::from_be_bytes(self.0[..8].try_into().unwrap())
    }

    pub fn get_segments(&self) -> Vec<ReadOnlySegment> {
        let mut v = Vec::new();
        let mut offset = 8;
        while offset < self.0.len() {
            let id = {
                let id = u64::from_be_bytes(self.0[offset..offset+8].try_into().unwrap());
                offset += 8;
                SeriesId(id)
            };

            let next_block = usize::from_be_bytes(self.0[offset..offset+8].try_into().unwrap());
            offset += 8;

            let segment_sz = usize::from_be_bytes(self.0[offset..offset+8].try_into().unwrap());
            offset += 8;

            let start = offset;
            let end = offset + segment_sz;

            v.push(ReadOnlySegment {
                id,
                next_block,
                start,
                end,
                ro_bytes: self.clone(),
            });
        }
        v
    }
}


