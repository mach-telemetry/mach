use crate::{
    compression::Compression,
    id::SeriesId,
    segment::FlushSegment,
    utils::wp_lock::{WpLock, NoDealloc},
    utils::byte_buffer::ByteBuffer,
    utils::random_id,
};
use std::sync::{Arc, RwLock, Mutex, mpsc::{Sender, Receiver, channel}, atomic::{AtomicU64, AtomicUsize, Ordering::SeqCst}};
use std::mem;
use std::time::{Duration, SystemTime};
use dashmap::DashMap;
use lazy_static::lazy_static;
use rdkafka::{
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord},
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
};
use crossbeam_queue::SegQueue;
use kafka::client::{
    KafkaClient,
    ProduceMessage,
    RequiredAcks
};
use rand::{Rng, thread_rng};

fn make_topic(bootstrap_servers: &str, topic: &str) {
    println!("Creating topic");
    let client: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers.to_string())
        .create()
        .unwrap();
    let admin_opts = AdminOptions::new().request_timeout(Some(std::time::Duration::from_secs(5)));
    let topics = &[NewTopic {
        name: topic,
        num_partitions: 3,
        replication: TopicReplication::Fixed(3),
        config: Vec::new(),
    }];
    futures::executor::block_on(client.create_topics(topics, &admin_opts)).unwrap();
    println!("topic created: {}", TOPIC.as_str());
}


//
//fn default_producer(bootstraps: &str) -> FutureProducer {
//    println!("making producer to bootstraps: {}", bootstraps);
//    let producer: FutureProducer = ClientConfig::new()
//        .set("bootstrap.servers", bootstraps)
//        .set("message.max.bytes", "1000000000")
//        .set("message.copy.max.bytes", "5000000")
//        .set("compression.type", "none")
//        .set("acks", "all")
//        .set("message.timeout.ms", "10000")
//        .create()
//        .unwrap();
//    println!("producers made");
//    producer
//}

fn kafka_client(bootstraps: Vec<String>) -> KafkaClient {
    let bootstraps = vec![
        "localhost:9093".to_owned(),
        "localhost:9094".to_owned(),
        "localhost:9095".to_owned()
    ];
    let mut client = KafkaClient::new(bootstraps);
    client.load_metadata_all().unwrap();
    client
}

lazy_static! {
    static ref BOOTSTRAPS: String = String::from("localhost:9093,localhost:9094,localhost:9095");
    static ref TOPIC: String = random_id();
}

const FLUSHERS: usize = 4;
pub static TOTAL_MB_WRITTEN: AtomicUsize = AtomicUsize::new(0);
static QUEUE_LEN: AtomicUsize = AtomicUsize::new(0);

struct Flusher {
    block_list: Arc<BlockList>,
}

fn flush_worker(chan: crossbeam::channel::Receiver<Arc<ReadOnlyBlock>>) {
    use rand::Rng;
    use std::convert::TryInto;
    //let mut buf = Vec::new();
    let bootstraps = vec![
        "localhost:9093".to_owned(),
        "localhost:9094".to_owned(),
        "localhost:9095".to_owned()
    ];
    let mut client = KafkaClient::new(bootstraps);
    client.load_metadata_all().unwrap();
    while let Ok(block) = chan.recv() {
        QUEUE_LEN.fetch_sub(1, SeqCst);
        let mut guard = block.inner.lock().unwrap();
        let (partition, offset): (usize, usize) = match &*guard {
            InnerReadOnlyBlock::Bytes(x) => {
                let part: i32= rand::thread_rng().gen_range(0..3);
                let req = vec![ProduceMessage::new(&*TOPIC, part, None, Some(&x))];
                let resp = client.produce_messages(RequiredAcks::All, Duration::from_millis(1000), req.as_slice()).unwrap();
                let offset = resp[0].partition_confirms[0].offset.unwrap();
                TOTAL_MB_WRITTEN.fetch_add(x.len(), SeqCst);
                (part.try_into().unwrap(), offset.try_into().unwrap())
            },
            _ => unimplemented!(),
        };
        *guard = InnerReadOnlyBlock::Offset(partition, offset);
        //*guard = InnerReadOnlyBlock::Offset(0, 0);
    }
}

pub struct List {
    head: AtomicU64,
}
impl List {
    pub fn new() -> Self {
        List {
            head: AtomicU64::new(u64::MAX)
        }
    }
}

pub struct BlockList {
    head_block: WpLock<Block>,
    tail: AtomicU64,
    block_map: DashMap<u64, (SystemTime, Arc<ReadOnlyBlock>)>,
    flushers: Vec<crossbeam::channel::Sender<Arc<ReadOnlyBlock>>>,
}

impl BlockList {
    pub fn new() -> Self {
        make_topic(BOOTSTRAPS.as_str(), TOPIC.as_str());
        let mut flushers = Vec::new();
        for _ in 0..FLUSHERS {
            let (tx, rx) = crossbeam::channel::unbounded();
            std::thread::spawn(move || {
                flush_worker(rx);
            });
            flushers.push(tx);
        }
        BlockList {
            head_block: WpLock::new(Block::new()),
            tail: AtomicU64::new(0),
            block_map: DashMap::new(),
            flushers,
        }
    }

    //pub fn push(&self, key: u64, block: Block) {
    //    let key = self.head.store(key) + 1;
    //    self.map.insert(key, block);
    //    self.head.store(key, SeqCst);
    //}

    pub fn push(
        &self,
        list: &List,
        series_id: SeriesId,
        segment: &FlushSegment,
        compression: &Compression,
    ) {
        //println!("PUSHING HERE");

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

        if is_full {
            //println!("is full");
            // Safety: there should be only one writer and it should be doing this push
            let copy = Arc::new(unsafe { self.head_block.unprotected_read().as_read_only() });
            self.block_map.insert(block_id, (SystemTime::now(), copy.clone()));
            self.flushers[thread_rng().gen_range(0..FLUSHERS)].send(copy).unwrap();
            println!("FLUSH QUEUE LEN: {}", QUEUE_LEN.fetch_add(1, SeqCst));

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
            len: AtomicUsize::new(0),
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
        ReadOnlyBlock::from_inner(InnerReadOnlyBlock::Bytes(bytes))
    }
}

enum InnerReadOnlyBlock {
    Bytes(Box<[u8]>),
    Offset(usize, usize),
}

pub struct ReadOnlyBlock {
    inner: Mutex<InnerReadOnlyBlock>,
}

impl ReadOnlyBlock {
    fn from_inner(inner: InnerReadOnlyBlock) -> Self {
        Self {
            inner: Mutex::new(inner)
        }
    }
}
