use std::sync::{Arc, atomic::{AtomicUsize, Ordering::SeqCst}};
use crate::{
    utils::wp_lock::{WpLock, NoDealloc},
    mem_list::{ReadOnlyBlock, Error},
};

struct InnerBuffer {
    data: Box<[Arc<ReadOnlyBlock>]>,
    offset: usize
}

impl InnerBuffer {
    fn push(&mut self, item: Arc<ReadOnlyBlock>) {
        let idx = (self.offset + 1) % self.data.len();
        self.data[idx] = item;
        self.offset += 1;
    }

    fn snapshot(&self) -> (Box<[Arc<ReadOnlyBlock>]>, usize) {
        (self.data.clone(), self.offset)
    }
}

unsafe impl NoDealloc for InnerBuffer {}

pub struct IndexList {
    inner: WpLock<InnerBuffer>
}

impl IndexList {
    pub fn push(&self, item: Arc<ReadOnlyBlock>) {
        self.inner.protected_write().push(item);
    }

    pub fn snapshot(&self) -> Result<Vec<Arc<ReadOnlyBlock>>, Error> {
        let guard = self.inner.protected_read();
        let (data, offset) = guard.snapshot();
        if guard.release().is_err() {
            Err(Error::Snapshot)
        } else {
            let mut v = Vec::new();
            for off in offset..offset + data.len() {
                v.push(data[off % data.len()].clone());
            }
            Ok(v)
        }
    }
}






