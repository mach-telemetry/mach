use std::sync::{Arc, RwLock, Mutex, atomic::{AtomicU64, AtomicUsize, Ordering::SeqCst}};
use std::mem;

pub struct MemList {
    head: Arc<RwLock<Node>>,
    tail: Arc<RwLock<Node>>,
}

impl MemList {
    pub fn new(bytes: Box<[u8]>, len: usize) -> Self {
        let data = Data { bytes, len };
        let node = Node {
            persistent_offset: Arc::new(AtomicU64::new(u64::MAX)),
            data: Arc::new(RwLock::new(data)),
            prev: Arc::new(RwLock::new(None)),
            next: Arc::new(RwLock::new(None)),
        };
        let head = Arc::new(RwLock::new(node));
        let tail = head.clone();

        Self {
            head,
            tail,
        }
    }

    pub fn push(&self, bytes: Box<[u8]>, len: usize) {
        let data = Data { bytes, len };
        let next = self.head.read().unwrap().clone();
        let node = Node {
            persistent_offset: Arc::new(AtomicU64::new(u64::MAX)),
            data: Arc::new(RwLock::new(data)),
            prev: Arc::new(RwLock::new(None)),
            next: Arc::new(RwLock::new(Some(next.clone()))),
        };
        next.set_prev(node.clone());
        self.set_head(node);
    }

    fn set_head(&self, node: Node) {
        let _ = mem::replace(&mut *self.head.write().unwrap(), node);
    }

    fn set_tail(&self, node: Node) {
        let _ = mem::replace(&mut *self.tail.write().unwrap(), node);
    }
}

struct Data {
    len: usize,
    bytes: Box<[u8]>,
}

#[derive(Clone)]
struct Node {
    persistent_offset: Arc<AtomicU64>,
    data: Arc<RwLock<Data>>,
    prev: Arc<RwLock<Option<Node>>>,
    next: Arc<RwLock<Option<Node>>>,
}

impl Node {
    fn set_prev(&self, node: Node) {
        assert!(mem::replace(&mut *self.prev.write().unwrap(), Some(node)).is_none());
    }

    fn set_next(&self, node: Node) {
        assert!(mem::replace(&mut *self.next.write().unwrap(), Some(node)).is_none());
    }

    fn set_persistent_offset(&self, val: u64) {
        self.persistent_offset.store(val, SeqCst);
    }
}


