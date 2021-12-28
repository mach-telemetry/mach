pub mod file;
pub mod kafka;

use crate::{segment, tags::Tags};
use dashmap::DashMap;
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{
        atomic::{AtomicU64, Ordering::SeqCst},
        Arc,
    },
};

#[derive(Debug)]
pub enum Error {
    SeriesNotFound,
    SeriesReinitialized,
    Segment(segment::Error),
}

impl From<segment::Error> for Error {
    fn from(item: segment::Error) -> Self {
        Error::Segment(item)
    }
}

pub enum PushStatus {
    Done,
}
