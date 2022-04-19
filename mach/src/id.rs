use crate::utils::random_id;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::ops::Deref;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct WriterId(pub String);

impl WriterId {
    pub fn inner(&self) -> &str {
        self.0.as_str()
    }

    pub fn random() -> Self {
        WriterId(random_id())
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl Deref for WriterId {
    type Target = str;
    fn deref(&self) -> &str {
        self.inner()
    }
}

#[derive(Copy, Clone, Eq, Debug, PartialEq, Hash, Serialize, Deserialize)]
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

#[derive(Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct SeriesRef(pub usize);

impl Deref for SeriesRef {
    type Target = usize;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SeriesRef {
    pub fn inner(&self) -> usize {
        self.0
    }
}
