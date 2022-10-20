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
pub struct SourceId(pub u64);

impl Deref for SourceId {
    type Target = u64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<u64> for SourceId {
    fn from(id: u64) -> Self {
        SourceId(id as u64)
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct SourceRef(pub u64);

impl Deref for SourceRef {
    type Target = u64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<u64> for SourceRef {
    fn from(id: u64) -> Self {
        SourceRef(id)
    }
}
