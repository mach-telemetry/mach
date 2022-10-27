use crate::{
    constants::{HEAP_SZ, SEG_SZ},
    field_type::FieldType,
    sample::SampleType,
};
use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;
use std::ops::{Deref, DerefMut};

pub type SegmentArray = [[u8; 8]; SEG_SZ];

#[inline]
pub fn zero_segment_array() -> SegmentArray {
    [[0u8; 8]; SEG_SZ]
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Column {
    #[serde(with = "BigArray")]
    data: SegmentArray,
}

impl Deref for Column {
    type Target = [[u8; 8]];
    fn deref(&self) -> &Self::Target {
        &self.data[..]
    }
}

impl DerefMut for Column {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data[..]
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Segment {
    pub len: usize,
    pub heap_len: usize,
    #[serde(with = "BigArray")]
    pub ts: [u64; SEG_SZ],
    pub heap: Box<[u8]>,
    pub data: Vec<Column>,
    pub types: Vec<FieldType>,
}

impl Segment {
    pub fn field_idx(&self, field: usize, idx: usize) -> SampleType {
        let value = self.data[field][idx];
        let field_type = self.types[field];
        match field_type {
            FieldType::Bytes => {
                SampleType::from_field_item(field_type, value, Some(&self.heap[..]))
            }
            _ => SampleType::from_field_item(field_type, value, None),
        }
    }

    pub fn new_empty() -> Self {
        Self {
            len: 0,
            heap_len: 0,
            ts: [0u64; 256],
            data: Vec::new(),
            heap: vec![0u8; HEAP_SZ].into_boxed_slice(),
            types: Vec::new(),
        }
    }

    pub fn new(ts: &[u64], data: &[&SegmentArray], heap: &[u8], types: &[FieldType]) -> Self {
        let len = ts.len();
        let nvars = types.len();
        let heap_len = heap.len();

        assert!(len <= SEG_SZ);
        assert!(heap_len <= HEAP_SZ);
        assert_eq!(data.len(), nvars);
        for d in data.iter() {
            assert_eq!(d.len(), len);
        }

        let mut s = Self::new_empty();
        s.types.extend_from_slice(types);
        s.ts[..len].copy_from_slice(ts);
        for d in data.iter() {
            s.data.push(Column { data: **d });
        }
        s.heap[..heap_len].copy_from_slice(heap);
        s.len = len;
        s.heap_len = heap_len;
        s
    }
}
