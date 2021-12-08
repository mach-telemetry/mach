use std::sync::{
    atomic::{AtomicUsize, Ordering::SeqCst},
    Arc,
};

pub enum Error {
    VersionError,
}

pub trait ListNode: Clone {
    fn next_node(&self) -> Self;
    fn get_data(&self) -> Box<[u8]>;
    fn get_metadata(&self) -> Metadata;
}

pub struct Metadata {
    pub tsid: u64,
    pub nvars: u64,
    pub mint: u64,
    pub maxt: u64,
    pub segment_metadata: Vec<SegmentMetadata>,
}

pub struct SegmentMetadata {
    pub mint: u64,
    pub maxt: u64,
    pub len: u64,
}

struct InnerList<T: ListNode> {
    inner: T,
    version: AtomicUsize,
}

impl<T: ListNode> InnerList<T> {
    fn update(&mut self, t: T) {
        self.inner = t;
        self.version.fetch_add(1, SeqCst);
    }

    fn load(&self) -> Result<T, Error> {
        let v = self.version.load(SeqCst);
        let node = self.inner.clone();
        if v == self.version.load(SeqCst) {
            Ok(node)
        } else {
            Err(Error::VersionError)
        }
    }
}

pub struct List<T: ListNode> {
    inner: Arc<InnerList<T>>,
}

impl<T: ListNode> List<T> {
    pub fn update(&mut self, t: T) {
        // Safety: Safe because of versioning
        unsafe {
            Arc::get_mut_unchecked(&mut self.inner).update(t);
        }
    }

    pub fn reader(&self) -> ListReader<T> {
        ListReader {
            inner: self.inner.clone(),
        }
    }
}

#[derive(Clone)]
pub struct ListReader<T: ListNode> {
    inner: Arc<InnerList<T>>,
}

impl<T: ListNode> ListReader<T> {
    pub fn load(&self) -> Result<T, Error> {
        self.load()
    }
}
