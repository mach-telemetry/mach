use crate::{
    mem_list::{BlockListEntry, Error, ReadOnlyBlock, BOOTSTRAPS, TOPIC},
    utils::{
        kafka,
        wp_lock::{NoDealloc, WpLock},
    },
};
use rand::Rng;
use std::mem::MaybeUninit;
use std::sync::{Arc, RwLock, atomic::{AtomicUsize, Ordering::SeqCst}};
use crossbeam::channel::{Sender, Receiver, unbounded};

fn flusher(channel: Receiver<Arc<RwLock<ListItem>>>, mut producer: kafka::Producer) {
    while let Ok(list_item) = channel.recv() {
        let guard = list_item.read().unwrap();
        match &*guard {
            ListItem::Offset{..} => unimplemented!(),
            ListItem::Block { data, next } => {
                let last = match &*next.read().unwrap() {
                    ListItem::Offset{partition, offset} => (*partition, *offset),
                    _ => panic!("Expected offset, got unflushed data"),
                };
                let mut v = Vec::new();
                for item in data.iter() {
                    item.flush(&mut producer);
                    let x = item.partition_offset();
                    v.push(ReadOnlyBlock::Offset(x.0, x.1));
                }
                let to_serialize = SourceBlocks {
                    idx: v.len(),
                    data: v,
                    next: last,
                };
                let bytes = bincode::serialize(&to_serialize).unwrap();
                let part: i32 = rand::thread_rng().gen_range(0..3);
                let (partition, offset) = producer.send(&*TOPIC, part, &bytes);
                drop(guard);
                *list_item.write().unwrap() = ListItem::Offset { partition, offset };
            }
        }
    }
}

enum ListItem {
    Block {
        data: Box<[Arc<BlockListEntry>]>,
        next: Arc<RwLock<ListItem>>,
    },
    Offset {
        partition: i32,
        offset: i64,
    }
}

struct InnerBuffer {
    data: Box<[MaybeUninit<Arc<BlockListEntry>>]>,
    offset: AtomicUsize,
    flusher: Sender<Arc<RwLock<ListItem>>>,
    last: Arc<RwLock<ListItem>>,
}

impl InnerBuffer {
    fn push(&mut self, item: Arc<BlockListEntry>) {
        let offset = self.offset.load(SeqCst);
        let idx = offset % self.data.len();
        self.data[idx].write(item);
        self.offset.fetch_add(1, SeqCst);
        if (offset + 1) % self.data.len() == 0 {
            unsafe {
                self.full_flush();
                //self.tmp_flush();
            }
        }
    }

    // for memory utilization experiment
    //unsafe fn tmp_flush(&mut self) {
    //    for item in self.data.iter() {
    //        let r = item.assume_init_ref().clone();
    //        self.tmp.push(r);
    //    }
    //}

    unsafe fn full_flush(&mut self) {
        let mut v = Vec::new();
        for item in self.data.iter() {
            v.push(item.assume_init_ref().clone());
        }
        let item = Arc::new(RwLock::new(ListItem::Block {
            data: v.into_boxed_slice(),
            next: self.last.clone(),
        }));
        self.flusher.send(item.clone()).unwrap();
        self.last = item;

        //let mut v = vec![self.last];
        //for item in self.data.iter() {
        //    let r = item.assume_init_ref();
        //    r.flush(&mut self.producer);
        //    let p = r.partition_offset();
        //    v.push(p);
        //}
        //let bytes = bincode::serialize(&v).unwrap();
        //let part: i32 = rand::thread_rng().gen_range(0..3);
        //self.last = self.producer.send(&*TOPIC, part, &bytes);
    }

    fn snapshot(&self) -> SourceBlocks {
        let mut v = Vec::with_capacity(256);
        let end = self.offset.load(SeqCst);
        let start = if end < 256 { 0 } else { end - 256 };
        for off in start..end {
            // Safety: offset ensures prior items are inited
            unsafe {
                let inner = self.data[off % 256].assume_init_ref().inner();
                v.push(inner.into());
            }
        }
        let mut current = self.last.clone();
        #[allow(unused_assignments)]
        let mut next = (-1, -1);

        loop { // loop will end because this was inited with Offset.
            let guard = current.read().unwrap();
            match &*guard {
                ListItem::Offset{ partition, offset } => {
                    next = (*partition, *offset);
                    break;
                }
                ListItem::Block{ data, next } => {
                    data.iter().cloned().for_each(|x| v.push(x.inner().into()));
                    let x = next.clone();
                    drop(guard);
                    current = x;
                    continue;
                }
            }
        }
        let idx = v.len();
        SourceBlocks {
            data: v,
            next,
            idx,
        }
    }

    fn new() -> Self {
        let mut vec = Vec::with_capacity(256);
        vec.resize_with(256, MaybeUninit::uninit);
        let producer = kafka::Producer::new(&*BOOTSTRAPS);
        let (tx, rx) = unbounded();
        std::thread::spawn(move || {
            flusher(rx, producer)
        });
        Self {
            data: vec.into_boxed_slice(),
            offset: AtomicUsize::new(0),
            flusher: tx,
            last: Arc::new(RwLock::new(ListItem::Offset { partition: -1, offset: -1 } )),
            // for memory utilization experiment
            //tmp: Vec::new(),
        }
    }
}

unsafe impl NoDealloc for InnerBuffer {}

pub struct SourceBlockList {
    inner: WpLock<InnerBuffer>,
}

impl SourceBlockList {
    pub fn new() -> Self {
        Self {
            inner: WpLock::new(InnerBuffer::new()),
        }
    }

    pub fn push(&self, item: Arc<BlockListEntry>) {
        self.inner.protected_write().push(item);
    }

    pub fn snapshot(&self) -> Result<SourceBlocks, Error> {
        let guard = self.inner.protected_read();
        let data = guard.snapshot();
        if guard.release().is_err() {
            Err(Error::Snapshot)
        } else {
            Ok(data)
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct SourceBlocks {
    data: Vec<ReadOnlyBlock>,
    next: (i32, i64),
    idx: usize,
}

impl SourceBlocks {
    pub fn next_block(&mut self, consumer: &mut kafka::BufferedConsumer) -> Option<&ReadOnlyBlock> {
        if self.idx > 0 {
            self.idx -= 1;
            Some(&self.data[self.idx])
        } else if self.next.0 == -1 || self.next.1 == -1 {
            None
        } else {
            let next_blocks: SourceBlocks =
                bincode::deserialize(&*consumer.get(self.next.0, self.next.1)).unwrap();
            self.idx = next_blocks.data.len();
            self.data = next_blocks.data;
            self.next = next_blocks.next;
            self.next_block(consumer)
        }
    }
}
