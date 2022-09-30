use lazy_static::*;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

lazy_static! {
    static ref WRITER_ID: AtomicUsize = AtomicUsize::new(0);
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct WriterId(pub usize);

impl WriterId {
    //pub fn inner(&self) -> &str {
    //    self.0.as_str()
    //}

    //pub fn random() -> Self {
    //    WriterId(random_id())
    //}

    pub fn new() -> Self {
        WriterId(WRITER_ID.fetch_add(1, SeqCst))
    }
}

//impl Deref for WriterId {
//    type Target = str;
//    fn deref(&self) -> &str {
//        self.inner()
//    }
//}

#[derive(Copy, Clone, Eq, Debug, PartialEq, Hash, Serialize, Deserialize)]
pub struct SeriesId(pub u64);

impl Deref for SeriesId {
    type Target = u64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<usize> for SeriesId {
    fn from(id: usize) -> Self {
        SeriesId(id as u64)
    }
}

impl SeriesId {
    pub fn inner(&self) -> u64 {
        self.0
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct SeriesRef(pub usize);

impl Deref for SeriesRef {
    type Target = usize;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<usize> for SeriesRef {
    fn from(id: usize) -> Self {
        SeriesRef(id)
    }
}

impl SeriesRef {
    pub fn inner(&self) -> usize {
        self.0
    }
}
