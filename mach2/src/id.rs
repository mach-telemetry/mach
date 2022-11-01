pub use crate::source::SourceId;
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

