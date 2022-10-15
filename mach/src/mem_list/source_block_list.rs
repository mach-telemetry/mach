use crate::{
    constants::*,
    id::SeriesId,
    mem_list::{
        BlockListEntry, ChunkBytes, ChunkBytesOrKafka, Error, ReadOnlyBlock, ReadOnlyBlock2,
        ReadOnlyBlock3, PENDING_UNFLUSHED_BYTES,
    },
    utils::{
        kafka,
        timer::*,
        wp_lock::{NoDealloc, WpLock},
    },
};
use crossbeam::channel::{unbounded, Receiver, Sender};
use rand::{thread_rng, Rng};
use std::mem::MaybeUninit;
use std::sync::{
    atomic::{AtomicUsize, Ordering::SeqCst},
    Arc, RwLock,
};
use std::thread;
use std::time::{Instant, Duration};

lazy_static::lazy_static! {
    static ref PENDING_UNFLUSHED_BLOCKLISTS: Arc<AtomicUsize> = {
        let x = Arc::new(AtomicUsize::new(0));
        let x2 = x.clone();
        thread::spawn(move || loop {
            let x = x2.load(SeqCst);
            println!("Pending unflushed blocklists: {}", x);
            thread::sleep(Duration::from_secs(1));
        });
        x
    };
    static ref LIST_ITEM_FLUSHER: Sender<(SeriesId, Arc<RwLock<ListItem>>)> = {
            let producer = kafka::Producer::new();
            let (tx, rx) = unbounded();
            std::thread::spawn(move || flusher(0, rx, producer));
            tx
    };
}

fn flusher(partition: i32, channel: Receiver<(SeriesId, Arc<RwLock<ListItem>>)>, mut producer: kafka::Producer) {
    let mut partition: i32= thread_rng().gen_range(0..PARTITIONS);
    let mut counter = 0;
    while let Ok((_id, list_item)) = channel.recv() {
        let (data, next): (Arc<[InnerListEntry]>, Arc<RwLock<ListItem>>) = {
            let guard = list_item.read().unwrap();
            match &*guard {
                ListItem::Kafka(_) => unimplemented!(),
                ListItem::Block(x) => {
                    let data = x.0.clone();
                    let next = x.1.clone();
                    (data, next)
                }
            }
        };
        let size_to_flush = data.len() * std::mem::size_of::<InnerListEntry>();
        let last = match &*next.read().unwrap() {
            ListItem::Kafka(entry) => entry.clone(),
            _ => panic!("Expected offset, got unflushed data"),
        };
        let mut v = Vec::new();
        println!("Flushing a source block's items");
        let now = Instant::now();
        for item in data.iter().rev() {
            item.block.flush(partition, &mut producer);
            counter += 1;
            let x = item.block.partition_offset();
            let (min_ts, max_ts) = (item.min_ts, item.max_ts);
            let block = ReadOnlyBlock::Offset(x);
            v.push(ReadOnlyBlock2 {
                min_ts,
                max_ts,
                block,
            });
        }
        println!("Flushing a source block's items {:?}", now.elapsed());
        let to_serialize = SourceBlocks2 {
            idx: 0,
            data: v,
            next: last,
        };
        let bytes = bincode::serialize(&to_serialize).unwrap();
        let kafka_entry = producer.send(partition, &bytes);
        counter += 1;
        //HISTORICAL_BLOCKS.insert(id, kafka_entry.clone());
        //drop(guard);
        *list_item.write().unwrap() = ListItem::Kafka(kafka_entry);
        PENDING_UNFLUSHED_BLOCKLISTS.fetch_sub(1, SeqCst);
        PENDING_UNFLUSHED_BYTES.fetch_sub(size_to_flush, SeqCst);

        if counter % 1000 == 0 {
            partition = thread_rng().gen_range(0..PARTITIONS);
        }
    }
    println!("BLOCKLIST FLUSHER EXIT");
}

enum ListItem {
    #[allow(clippy::type_complexity)]
    Block(Arc<(Arc<[InnerListEntry]>, Arc<RwLock<ListItem>>)>),
    Kafka(kafka::KafkaEntry),
}

#[derive(Clone)]
struct InnerListEntry {
    min_ts: u64,
    max_ts: u64,
    block: Arc<BlockListEntry>,
}

struct InnerData {
    data: Box<[MaybeUninit<InnerListEntry>; 256]>,
    offset: AtomicUsize,
}

impl InnerData {
    fn push(&mut self, min_ts: u64, max_ts: u64, item: Arc<BlockListEntry>) -> bool {
        let list_entry = InnerListEntry {
            min_ts,
            max_ts,
            block: item,
        };
        let offset = self.offset.load(SeqCst);
        let idx = offset % self.data.len();
        self.data[idx].write(list_entry);

        self.offset.fetch_add(1, SeqCst);

        (offset + 1) % 256 == 0
    }
}

unsafe impl NoDealloc for InnerData {}
unsafe impl Sync for InnerBuffer {} // last_update is only accessed by one writer

struct InnerBuffer {
    id: SeriesId,
    data: Arc<WpLock<InnerData>>,
    next: Arc<RwLock<Arc<RwLock<ListItem>>>>,
}

impl InnerBuffer {
    fn push(&self, min_ts: u64, max_ts: u64, item: Arc<BlockListEntry>) {
        let is_full = unsafe { self.data.unprotected_write().push(min_ts, max_ts, item) };
        if is_full {
            let copy: Arc<[InnerListEntry]> = unsafe {
                let guard = self.data.unprotected_read();
                MaybeUninit::slice_assume_init_ref(&guard.data[..]).into()
            };
            {
                PENDING_UNFLUSHED_BYTES.fetch_add(copy.len() * std::mem::size_of::<InnerListEntry>(), SeqCst);
                let mut guard = self.next.write().unwrap();
                let list_item = Arc::new(RwLock::new(ListItem::Block(Arc::new((
                    copy,
                    guard.clone(),
                )))));
                LIST_ITEM_FLUSHER
                    .send((self.id, list_item.clone()))
                    .unwrap();
                *guard = list_item;
                let _guard = self.data.protected_write(); // need to increment guard
            }
            PENDING_UNFLUSHED_BLOCKLISTS.fetch_add(1, SeqCst);
        }
    }

    fn snapshot(&self) -> Result<SourceBlocks2, Error> {
        // Get components to read
        let guard = self.data.protected_read();
        let len = guard.offset.load(SeqCst) % 256;
        //println!("Snapshot len: {}", len);
        let copy: Vec<InnerListEntry> =
            unsafe { MaybeUninit::slice_assume_init_ref(&guard.data[..len]).to_vec() };
        let current = self.next.read().unwrap().clone();
        if guard.release().is_err() {
            return Err(Error::Snapshot);
        }

        // Traverse list
        let mut v: Vec<ReadOnlyBlock2> = Vec::with_capacity(256);
        for item in copy.iter().rev() {
            v.push(ReadOnlyBlock2 {
                min_ts: item.min_ts,
                max_ts: item.max_ts,
                block: item.block.inner().into(),
            });
        }

        #[allow(unused_assignments)]
        let mut next = kafka::KafkaEntry::new();
        let mut current = current; // I know this is repetitive but merp

        let mut loop_counter = 0;
        loop {
            loop_counter += 1;
            if loop_counter > 100_000 {
                panic!("looping problem");
            }
            // loop will end because this was inited with Offset.
            let guard = current.read().unwrap();
            match &*guard {
                ListItem::Kafka(entry) => {
                    next = entry.clone();
                    break;
                }
                ListItem::Block(item) => {
                    item.0.iter().rev().cloned().for_each(|x| {
                        v.push(ReadOnlyBlock2 {
                            min_ts: x.min_ts,
                            max_ts: x.max_ts,
                            block: x.block.inner().into(),
                        });
                    });
                    let x = item.1.clone();
                    drop(guard);
                    current = x;
                    continue;
                }
            }
        }
        let idx = 0;
        Ok(SourceBlocks2 { data: v, next, idx })
    }

    fn new(id: SeriesId) -> Self {
        let offset = AtomicUsize::new(0);
        let data = Box::new(MaybeUninit::uninit_array());
        let data = Arc::new(WpLock::new(InnerData { data, offset }));
        let next = Arc::new(RwLock::new(Arc::new(RwLock::new(ListItem::Kafka(
            kafka::KafkaEntry::new(),
        )))));
        //let periodic = Arc::new(Mutex::new(last.clone()));
        Self { id, data, next }
    }
}

unsafe impl NoDealloc for InnerBuffer {}

pub struct SourceBlockList {
    inner: InnerBuffer,
}

impl SourceBlockList {
    pub fn new(id: SeriesId) -> Self {
        Self {
            inner: InnerBuffer::new(id),
        }
    }

    pub fn push(&self, min_ts: u64, max_ts: u64, item: Arc<BlockListEntry>) {
        self.inner.push(min_ts, max_ts, item);
    }

    pub fn snapshot(&self) -> Result<SourceBlocks2, Error> {
        self.inner.snapshot()
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct SourceBlocks2 {
    pub data: Vec<ReadOnlyBlock2>,
    pub next: kafka::KafkaEntry,
    pub idx: usize,
}

impl SourceBlocks2 {
    pub fn next_block(&mut self) -> Option<&ReadOnlyBlock2> {
        let _timer_1 = ThreadLocalTimer::new("SourceBlocks::next_block");
        if self.idx < self.data.len() {
            let idx = self.idx;
            self.idx += 1;
            Some(&self.data[idx])
        } else if self.next.is_empty() {
            None
        } else {
            let mut vec = Vec::new();
            {
                let _timer_2 = ThreadLocalTimer::new("SourceBlocks::next_block loading from kafka");
                self.next.load(&mut vec).unwrap()
            };
            let next_blocks: SourceBlocks2 = {
                let _timer_2 = ThreadLocalTimer::new("SourceBlocks::next_block deserializing");
                bincode::deserialize(vec.as_slice()).unwrap()
            };
            self.idx = 0;
            self.data = next_blocks.data;
            self.next = next_blocks.next;
            self.next_block()
        }
    }
    pub fn to_sourceblocks3(self, id: u64) -> SourceBlocks3 {
        SourceBlocks3::from_sourceblocks2(self, id)
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct SourceBlocks3 {
    pub id: u64,
    pub data: Vec<ReadOnlyBlock3>,
    pub next: kafka::KafkaEntry,
    pub idx: usize,
}

impl SourceBlocks3 {
    pub fn from_sourceblocks2(source: SourceBlocks2, id: u64) -> Self {
        let data = source
            .data
            .iter()
            .map(|x| x.to_readonlyblock_3(id))
            .collect();
        let next = source.next;
        Self {
            id,
            data,
            next,
            idx: 0,
        }
    }

    pub fn next_block(&mut self) -> Option<&ReadOnlyBlock3> {
        if self.idx < self.data.len() {
            let idx = self.idx;
            self.idx += 1;
            Some(&self.data[idx])
        } else if self.next.is_empty() {
            None
        } else {
            let mut vec = Vec::new();
            self.next.load(&mut vec).unwrap();
            let next_blocks: SourceBlocks2 = bincode::deserialize(vec.as_slice()).unwrap();
            let next_blocks = SourceBlocks3::from_sourceblocks2(next_blocks, self.id);
            self.idx = 0;
            self.data = next_blocks.data;
            self.next = next_blocks.next;
            self.next_block()
        }
    }
}

//#[derive(Clone, serde::Serialize, serde::Deserialize)]
//pub struct SourceBlocks {
//    data: Vec<ReadOnlyBlock>,
//    next: kafka::KafkaEntry,
//    idx: usize,
//    //prefetch: Option<Vec<kafka::KafkaEntry>>,
//    //pub cached_messages_read: usize,
//    //pub messages_read: usize,
//}
//
//impl SourceBlocks {
//    pub fn next_block(&mut self) -> Option<&ReadOnlyBlock> {
//        let _timer_1 = ThreadLocalTimer::new("SourceBlocks::next_block");
//        //match &self.prefetch {
//        //    Some(x) => kafka::prefetch(x.as_slice()),
//        //    None => {}
//        //}
//        if self.idx < self.data.len() {
//            let idx = self.idx;
//            self.idx += 1;
//            Some(&self.data[idx])
//        } else if self.next.is_empty() {
//            //println!("source block list exhausted");
//            None
//        } else {
//            let mut vec = Vec::new();
//            let stats = {
//                let _timer_2 = ThreadLocalTimer::new("SourceBlocks::next_block loading from kafka");
//                self.next.load(&mut vec).unwrap()
//            };
//            //self.cached_messages_read +=  stats.cached_messages_read;
//            //self.messages_read +=  stats.messages_read;
//            let next_blocks: SourceBlocks = {
//                let _timer_2 = ThreadLocalTimer::new("SourceBlocks::next_block deserializing");
//                bincode::deserialize(vec.as_slice()).unwrap()
//            };
//            self.idx = 0;
//            self.data = next_blocks.data;
//            self.next = next_blocks.next;
//            self.next_block()
//        }
//    }
//}
