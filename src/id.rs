use serde::{Deserialize, Serialize};
use std::ops::Deref;

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
pub struct WriterId(pub usize);

impl WriterId {
    pub fn inner(&self) -> usize {
        self.0
    }
}

impl Deref for WriterId {
    type Target = usize;
    fn deref(&self) -> &usize {
        &self.0
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct SeriesId(pub u64);

impl Deref for SeriesId {
    type Target = u64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SeriesId {
    pub fn inner(&self) -> u64 {
        self.0
    }
}
