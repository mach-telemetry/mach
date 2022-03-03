pub use crate::utils::bytes::*;
use std::alloc::{alloc, alloc_zeroed, dealloc, Layout};
use std::convert::{AsRef, TryInto};
use std::mem::{align_of, size_of, ManuallyDrop};

pub enum Type {
    U64(u64),
    F64(f64),
    Bytes(*const u8),
}

unsafe impl Sync for Type {}
unsafe impl Send for Type {}

#[derive(Copy, Clone)]
pub struct Sample<const V: usize> {
    pub timestamp: u64,
    pub values: [[u8; 8]; V],
}

impl<const V: usize> Sample<V> {
    pub fn new(timestamp: u64, values: [[u8; 8]; V]) -> Self {
        Sample { timestamp, values }
    }

    pub fn from_f64(timestamp: u64, data: [f64; V]) -> Self {
        let mut values = [[0; 8]; V];
        for i in 0..V {
            values[i] = data[i].to_be_bytes();
        }
        Sample { timestamp, values }
    }

    pub fn from_u64(timestamp: u64, data: [u64; V]) -> Self {
        let mut values = [[0; 8]; V];
        for i in 0..V {
            values[i] = data[i].to_be_bytes();
        }
        Sample { timestamp, values }
    }

    pub fn from_bytes(timestamp: u64, mut data: [Bytes; V]) -> Self {
        let mut values = [[0; 8]; V];
        let mut counter = 0;
        for item in data {
            values[counter] = item.into_sample_entry();
            counter += 1;
        }
        Sample { timestamp, values }
    }
}
