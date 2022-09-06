use crate::{
    id::SeriesId,
    mem_list::{add_flush_worker, BlockListEntry, Error, ReadOnlyBlock},
    timer::*,
    utils::{
        kafka,
        wp_lock::{NoDealloc, WpLock},
    },
};
use crossbeam::channel::{unbounded, Receiver, Sender};
use dashmap::DashMap;
use std::cell::RefCell;
use std::mem::MaybeUninit;
use std::sync::{
    atomic::{AtomicUsize, Ordering::SeqCst},
    Arc, Mutex, RwLock,
};
use std::time::{Duration, Instant};

lazy_static::lazy_static! {
    pub static ref PENDING_UNFLUSHED_BLOCKLISTS: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    static ref LIST_ITEM_FLUSHER: Sender<(SeriesId, Arc<RwLock<ListItem>>)> = {
        let producer = kafka::Producer::new();
        let (tx, rx) = unbounded();
        std::thread::spawn(move || flusher(rx, producer));
        tx
    };
    //pub static ref HISTORICAL_BLOCKS: HistoricalBlocksMap = HistoricalBlocksMap::new();
}

//pub struct HistoricalBlocks {
//    arr: [MaybeUninit<kafka::KafkaEntry>; 10],
//    sz: usize,
//}
//
//impl HistoricalBlocks {
//    fn new() -> Self {
//        Self {
//            arr: MaybeUninit::uninit_array::<10>(),
//            sz: 0
//        }
//    }
//
//    fn push(&mut self, entry: kafka::KafkaEntry) {
//        let idx = self.sz % 10;
//        if self.sz > 10 {
//            unsafe {
//                self.arr[idx].assume_init_drop();
//            }
//        }
//        self.arr[idx].write(entry);
//        self.sz += 1;
//    }
//
//    pub fn snapshot(&self) -> Vec<kafka::KafkaEntry> {
//        if self.sz <= 10 {
//            unsafe {
//                MaybeUninit::slice_assume_init_ref(&self.arr[..self.sz]).into()
//            }
//        } else {
//            let mut v = Vec::with_capacity(10);
//            for i in self.sz..self.sz + 10 {
//                let idx = i % 10;
//                v.push(unsafe {
//                    (&self.arr[idx]).assume_init_ref().clone()
//                });
//            }
//            v
//        }
//    }
//}
//
//pub struct HistoricalBlocksMap {
//    inner: Arc<DashMap<SeriesId, HistoricalBlocks>>
//}
//
//impl HistoricalBlocksMap {
//    fn new() -> Self {
//        Self {
//            inner: Arc::new(DashMap::new())
//        }
//    }
//
//    fn insert(&self, id: SeriesId, entry: kafka::KafkaEntry) {
//        let mut blocks = self.inner.entry(id).or_insert( {
//            HistoricalBlocks::new()
//        });
//        blocks.push(entry);
//    }
//
//    pub fn snapshot(&self, id: SeriesId) -> Option<Vec<kafka::KafkaEntry>> {
//        Some(self.inner.get(&id)?.snapshot())
//    }
//}

fn flusher(channel: Receiver<(SeriesId, Arc<RwLock<ListItem>>)>, mut producer: kafka::Producer) {
    while let Ok((id, list_item)) = channel.recv() {
        let (data, next) = {
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
        let last = match &*next.read().unwrap() {
            ListItem::Kafka(entry) => entry.clone(),
            _ => panic!("Expected offset, got unflushed data"),
        };
        let mut v = Vec::new();
        for item in data.iter().rev() {
            item.flush(&mut producer);
            let x = item.partition_offset();
            v.push(ReadOnlyBlock::Offset(x));
        }
        let to_serialize = SourceBlocks {
            idx: 0,
            data: v,
            next: last,
            //prefetch: HISTORICAL_BLOCKS.snapshot(id),
            //cached_messages_read: 0,
            //messages_read: 0,
        };
        let bytes = bincode::serialize(&to_serialize).unwrap();
        let kafka_entry = producer.send(&bytes);
        //HISTORICAL_BLOCKS.insert(id, kafka_entry.clone());
        //drop(guard);
        *list_item.write().unwrap() = ListItem::Kafka(kafka_entry);
        PENDING_UNFLUSHED_BLOCKLISTS.fetch_sub(1, SeqCst);
    }
}

enum ListItem {
    Block(Arc<(Arc<[Arc<BlockListEntry>]>, Arc<RwLock<ListItem>>)>),
    Kafka(kafka::KafkaEntry),
}

struct InnerData {
    data: Box<[MaybeUninit<Arc<BlockListEntry>>; 256]>,
    offset: AtomicUsize,
}

impl InnerData {
    fn push(&mut self, item: Arc<BlockListEntry>) -> bool {
        let offset = self.offset.load(SeqCst);
        let idx = offset % self.data.len();
        self.data[idx].write(item);
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
    fn push(&self, item: Arc<BlockListEntry>) {
        let is_full = unsafe { self.data.unprotected_write().push(item) };
        if is_full {
            let copy: Arc<[Arc<BlockListEntry>]> = unsafe {
                let guard = self.data.unprotected_read();
                MaybeUninit::slice_assume_init_ref(&guard.data[..]).into()
            };
            {
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

    //unsafe fn full_flush(&self) {
    //    let guard = self.data.unprotected_read();
    //    let data: Arc<[Arc<BlockListEntry>]> = {
    //        let mut arr: Arc<[MaybeUninit<Arc<BlockListEntry>>]> = Arc::new_uninit_slice(256);
    //        let arr_ref = Arc::get_mut_unchecked(&mut arr);
    //        for (a, b) in guard.data.iter().zip(arr_ref.iter_mut()) {
    //            b.write(a.assume_init_ref().clone());
    //        }
    //        arr.assume_init()
    //    };
    //    let item = Arc::new(RwLock::new(ListItem::Block {
    //        data,
    //        next: guard.last.clone(),
    //    }));
    //    drop(guard);
    //    UNFLUSHED_COUNT.fetch_add(1, SeqCst);
    //    LIST_ITEM_FLUSHER.send(item.clone()).unwrap();
    //    let mut guard = self.data.protected_write();
    //    guard.last = item.clone();
    //    //drop(guard);
    //    //let now = Instant::now();
    //    //let mut last_update = self.last_update.borrow_mut(); // this is the only thing accessing last_update
    //    //if now - *last_update >= Duration::from_secs_f64(0.5) {
    //    //    *self.periodic.lock().unwrap() = item.clone();
    //    //    *last_update = now;
    //    //}
    //}

    fn snapshot(&self) -> Result<SourceBlocks, Error> {
        // Get components to read
        let guard = self.data.protected_read();
        let len = guard.offset.load(SeqCst) % 256;
        //println!("Snapshot len: {}", len);
        let copy: Vec<Arc<BlockListEntry>> = unsafe {
            MaybeUninit::slice_assume_init_ref(&guard.data[..len])
                .iter()
                .cloned()
                .collect()
        };
        let current = self.next.read().unwrap().clone();
        if guard.release().is_err() {
            return Err(Error::Snapshot);
        }

        // Traverse list
        let mut v: Vec<ReadOnlyBlock> = Vec::with_capacity(256);
        for item in copy.iter().rev() {
            v.push(item.inner().into());
        }

        #[allow(unused_assignments)]
        let mut next = kafka::KafkaEntry::new();
        let mut current = current.clone(); // I know this is repetitive but merp

        loop {
            // loop will end because this was inited with Offset.
            let guard = current.read().unwrap();
            match &*guard {
                ListItem::Kafka(entry) => {
                    next = entry.clone();
                    break;
                }
                ListItem::Block(item) => {
                    println!("block len: {}", item.0.len());
                    item.0
                        .iter()
                        .rev()
                        .cloned()
                        .for_each(|x| v.push(x.inner().into()));
                    let x = item.1.clone();
                    drop(guard);
                    current = x;
                    continue;
                }
            }
        }
        let idx = 0;
        //let prefetch = HISTORICAL_BLOCKS.snapshot(self.id);
        Ok(SourceBlocks {
            data: v,
            next,
            idx,
            //prefetch,
            //cached_messages_read: 0,
            //messages_read: 0,
        })

        //let mut v = Vec::with_capacity(256);
        //let end = self.offset.load(SeqCst);
        //let guard = self.data.protected_read();
        //let start = if end < 256 { 0 } else { end - 256 };
        //for off in start..end {
        //    // Safety: offset ensures prior items are inited
        //    unsafe {
        //        let inner = guard.data[off % 256].assume_init_ref().inner();
        //        v.push(inner.into());
        //    }
        //}
        //let mut current = guard.last.clone();
        //match guard.release() {
        //    Ok(_) => {}
        //    Err(_) => return Err(Error::Snapshot),
        //}

        //#[allow(unused_assignments)]
        //let mut next = kafka::KafkaEntry::new();

        //loop {
        //    // loop will end because this was inited with Offset.
        //    let guard = current.read().unwrap();
        //    match &*guard {
        //        ListItem::Kafka(entry) => {
        //            next = entry.clone();
        //            break;
        //        }
        //        ListItem::Block { data, next } => {
        //            data.iter().cloned().for_each(|x| v.push(x.inner().into()));
        //            let x = next.clone();
        //            drop(guard);
        //            current = x;
        //            continue;
        //        }
        //    }
        //}
        //let idx = v.len();
        //Ok(SourceBlocks { data: v, next, idx })
    }

    //fn periodic_snapshot(&self) -> SourceBlocks {
    //    let mut v = Vec::with_capacity(256);
    //    let mut current = self.periodic.lock().unwrap().clone();
    //    #[allow(unused_assignments)]
    //    let mut next = kafka::KafkaEntry::new();
    //    loop {
    //        // loop will end because this was inited with Offset.
    //        let guard = current.read().unwrap();
    //        match &*guard {
    //            ListItem::Kafka(entry) => {
    //                next = entry.clone();
    //                break;
    //            }
    //            ListItem::Block { data, next } => {
    //                data.iter().cloned().for_each(|x| v.push(x.inner().into()));
    //                let x = next.clone();
    //                drop(guard);
    //                current = x;
    //                continue;
    //            }
    //        }
    //    }
    //    let idx = v.len();
    //    SourceBlocks { data: v, next, idx }
    //}

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
    //inner: Arc<Mutex<Vec<Arc<BlockListEntry>>>>
    inner: InnerBuffer,
}

impl SourceBlockList {
    pub fn new(id: SeriesId) -> Self {
        Self {
            //inner: Arc::new(Mutex::new(Vec::new())),
            inner: InnerBuffer::new(id),
        }
    }

    pub fn push(&self, item: Arc<BlockListEntry>) {
        //self.inner.lock().unwrap().push(item);
        self.inner.push(item);
    }

    pub fn snapshot(&self) -> Result<SourceBlocks, Error> {
        //let guard = self.inner.lock().unwrap();
        //let data = guard.iter().map(|x| x.inner().into()).collect();
        //let idx = guard.len();
        //let next = kafka::KafkaEntry::new();
        //Ok(SourceBlocks {
        //    data,
        //    next,
        //    idx
        //})
        self.inner.snapshot()
    }

    //pub fn periodic_snapshot(&self) -> SourceBlocks {
    //    // Safety: periodic snapshot accesses locked data
    //    self.inner.periodic_snapshot()
    //}
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct SourceBlocks {
    data: Vec<ReadOnlyBlock>,
    next: kafka::KafkaEntry,
    idx: usize,
    //prefetch: Option<Vec<kafka::KafkaEntry>>,
    //pub cached_messages_read: usize,
    //pub messages_read: usize,
}

impl SourceBlocks {
    pub fn next_block(&mut self) -> Option<&ReadOnlyBlock> {
        let _timer_1 = ThreadLocalTimer::new("SourceBlocks::next_block");
        //match &self.prefetch {
        //    Some(x) => kafka::prefetch(x.as_slice()),
        //    None => {}
        //}
        if self.idx < self.data.len() {
            let idx = self.idx;
            self.idx += 1;
            Some(&self.data[idx])
        } else if self.next.is_empty() {
            //println!("source block list exhausted");
            None
        } else {
            let mut vec = Vec::new();
            let stats = {
                let _timer_2 = ThreadLocalTimer::new("SourceBlocks::next_block loading from kafka");
                self.next.load(&mut vec).unwrap()
            };
            //self.cached_messages_read +=  stats.cached_messages_read;
            //self.messages_read +=  stats.messages_read;
            let next_blocks: SourceBlocks = {
                let _timer_2 = ThreadLocalTimer::new("SourceBlocks::next_block deserializing");
                bincode::deserialize(vec.as_slice()).unwrap()
            };
            self.idx = 0;
            self.data = next_blocks.data;
            self.next = next_blocks.next;
            self.next_block()
        }
    }
}
