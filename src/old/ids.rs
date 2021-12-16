use std::ops::Deref;
use serde::{Serialize, Deserialize};

#[derive(Hash, PartialEq, Eq, Copy, Clone, Serialize, Deserialize)]
pub struct SeriesId(pub u64);

impl Deref for SeriesId {
    type Target = u64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Hash, PartialEq, Eq, Copy, Clone, Serialize, Deserialize)]
pub struct RefId(pub u64);

impl Deref for RefId {
    type Target = u64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}


