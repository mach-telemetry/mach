use serde::{Deserialize, Serialize};
use std::ops::Deref;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct WriterId(pub u64);

impl Deref for WriterId {
    type Target = u64;
    fn deref(&self) -> &Self::Target {
        &self.0
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

impl From<u64> for SeriesId {
    fn from(id: u64) -> Self {
        SeriesId(id as u64)
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct SeriesRef(pub u64);

impl Deref for SeriesRef {
    type Target = u64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<u64> for SeriesRef {
    fn from(id: u64) -> Self {
        SeriesRef(id)
    }
}
