use std::sync::{Arc, atomic::{AtomicUsize, Ordering::SeqCst}};
use crate::{
    utils::{
        wp_lock::{WpLock, NoDealloc},
        kafka,
    },
    mem_list::{ReadOnlyBlock, Error, TOPIC, BOOTSTRAPS},
};
use std::mem::MaybeUninit;
use rand::Rng;
use std::convert::TryInto;

struct InnerBuffer {
    data: Box<[MaybeUninit<Arc<ReadOnlyBlock>>]>,
    offset: usize,
    producer: kafka::Producer,
    last: (usize, usize),
}

impl InnerBuffer {
    fn push(&mut self, item: Arc<ReadOnlyBlock>) {
        let idx = (self.offset + 1) % self.data.len();
        self.data[idx].write(item);
        self.offset += 1;
        if self.offset % self.data.len() == 0 {
            unsafe {
                self.full_flush();
            }
        }
    }

    unsafe fn full_flush(&mut self) {
        let mut v = Vec::new();
        v.push(self.last);
        for item in self.data.iter() {
            let r = item.assume_init_ref();
            r.flush(&mut self.producer);
            let p = r.partition_offset();
            v.push(p);
        }
        let bytes = bincode::serialize(&v).unwrap();
        let part: i32= rand::thread_rng().gen_range(0..3);
        let (p, o) = self.producer.send(&*TOPIC, part, &bytes);
        self.last = (p.try_into().unwrap(), o.try_into().unwrap());
    }

    fn snapshot(&self) -> Vec<Arc<ReadOnlyBlock>> {
        let mut v = Vec::with_capacity(256);
        let end = self.offset;
        let start = if end < 256 { 0 } else { end + 256 };
        for off in start..end {
            /// Safety: offset ensures prior items are inited
            unsafe {
                v.push(self.data[off % 256].assume_init_ref().clone());
            }
        }
        v
    }

    fn new() -> Self {
        let mut vec = Vec::with_capacity(256);
        vec.resize_with(256, MaybeUninit::uninit);
        let producer = kafka::Producer::new(&*BOOTSTRAPS);
        Self {
            data: vec.into_boxed_slice(),
            offset: 0,
            producer,
            last: (usize::MAX, usize::MAX),
        }
    }
}

unsafe impl NoDealloc for InnerBuffer {}

pub struct CircularBlockBuffer {
    inner: WpLock<InnerBuffer>
}

impl CircularBlockBuffer {
    pub fn new() -> Self {
        Self {
            inner: WpLock::new(InnerBuffer::new()),
        }
    }

    pub fn push(&self, item: Arc<ReadOnlyBlock>) {
        self.inner.protected_write().push(item);
    }

    pub fn snapshot(&self) -> Result<Vec<Arc<ReadOnlyBlock>>, Error> {
        let guard = self.inner.protected_read();
        let data = guard.snapshot();
        if guard.release().is_err() {
            Err(Error::Snapshot)
        } else {
            Ok(data)
        }
    }
}






