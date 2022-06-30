use std::sync::{Arc};
use crate::{
    utils::{
        wp_lock::{WpLock, NoDealloc},
        kafka,
    },
    mem_list::{BlockListEntry, ReadOnlyBlock, Error, TOPIC, BOOTSTRAPS},
};
use std::mem::MaybeUninit;
use rand::Rng;

struct InnerBuffer {
    data: Box<[MaybeUninit<Arc<BlockListEntry>>]>,
    offset: usize,
    producer: kafka::Producer,
    last: (i32, i64),

    // for memory utilization experiment
    //tmp: Vec<Arc<BlockListEntry>>,
}

impl InnerBuffer {
    fn push(&mut self, item: Arc<BlockListEntry>) {
        let idx = self.offset % self.data.len();
        self.data[idx].write(item);
        self.offset += 1;
        if self.offset % self.data.len() == 0 {
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
        let mut v = vec![self.last];
        for item in self.data.iter() {
            let r = item.assume_init_ref();
            r.flush(&mut self.producer);
            let p = r.partition_offset();
            v.push(p);
        }
        let bytes = bincode::serialize(&v).unwrap();
        let part: i32= rand::thread_rng().gen_range(0..3);
        self.last = self.producer.send(&*TOPIC, part, &bytes);
    }

    fn snapshot(&self) -> SourceBlocks {
        let mut v = Vec::with_capacity(256);
        let end = self.offset;
        let start = if end < 256 { 0 } else { end + 256 };
        for off in start..end {
            // Safety: offset ensures prior items are inited
            unsafe {
                let inner = self.data[off % 256].assume_init_ref().inner();
                v.push(inner.into());
            }
        }
        let idx = v.len();
        SourceBlocks {
            data: v,
            next: self.last,
            idx,
        }
    }

    fn new() -> Self {
        let mut vec = Vec::with_capacity(256);
        vec.resize_with(256, MaybeUninit::uninit);
        let producer = kafka::Producer::new(&*BOOTSTRAPS);
        Self {
            data: vec.into_boxed_slice(),
            offset: 0,
            producer,
            last: (-1, -1),
            // for memory utilization experiment
            //tmp: Vec::new(),
        }
    }
}

unsafe impl NoDealloc for InnerBuffer {}

pub struct SourceBlockList {
    inner: WpLock<InnerBuffer>
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

#[derive(serde::Serialize,serde::Deserialize)]
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
            let next_blocks: SourceBlocks = bincode::deserialize(&*consumer.get(self.next.0, self.next.1)).unwrap();
            self.idx = next_blocks.data.len();
            self.data = next_blocks.data;
            self.next = next_blocks.next;
            self.next_block(consumer)
        }
    }
}
