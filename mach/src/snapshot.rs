use crate::{
    constants::*, id::SeriesId, persistent_list::ListSnapshot, runtime::RUNTIME, sample::Type,
    segment::SegmentSnapshot, series::Series,
};
use serde::*;
use bincode::*;

#[derive(Serialize, Deserialize)]
pub struct Snapshot {
    segments: SegmentSnapshot,
    list: ListSnapshot,
}

impl Snapshot {
    pub fn new(segments: SegmentSnapshot, list: ListSnapshot) -> Self {
        Snapshot { segments, list }
    }

    pub fn to_bytes(self) -> Vec<u8> {
        let mut v = Vec::new();
        serialize_into(&mut v, &self).unwrap();
        v
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        deserialize_from(bytes).unwrap()
    }
}

pub struct SnaptshotIterator {
    segments: SegmentSnapshot,
    current_segment: usize,
}


