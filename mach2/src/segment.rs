use crate::{
    constants::{HEAP_SZ, SEG_SZ},
    field_type::FieldType,
    sample::SampleType,
};
use serde::{Deserialize, Serialize};

pub type SegmentArray = [[u8; 8]; SEG_SZ];

pub fn bytes_to_columns(bytes: &[u8]) -> &[SegmentArray] {
    let len = bytes.len();
    let ptr = bytes.as_ptr() as *const SegmentArray;

    assert_eq!(len % (8 * 256), 0);

    unsafe { std::slice::from_raw_parts(ptr, len / (8 * 256)) }
}

pub fn bytes_to_columns_mut(bytes: &mut [u8]) -> &mut [SegmentArray] {
    let len = bytes.len();
    let ptr = bytes.as_ptr() as *mut SegmentArray;

    assert_eq!(len % (8 * 256), 0);

    unsafe { std::slice::from_raw_parts_mut(ptr, len / (8 * 256)) }
}

pub struct SegmentRef<'a> {
    pub len: usize,
    pub heap_len: usize,
    pub timestamps: &'a [u64; SEG_SZ],
    pub heap: &'a [u8; HEAP_SZ],
    pub data: &'a [u8],
    pub types: &'a [FieldType],
}

impl<'a> SegmentRef<'a> {
    pub fn to_segment(&self) -> Segment {
        let len = self.len;
        let heap_len = self.heap_len;
        let timestamps: Box<[u64]> = self.timestamps[..].into();
        let heap: Box<[u8]> = self.heap[..].into();
        let data: Vec<u8> = self.data.into();
        let types: Vec<FieldType> = self.types.into();
        Segment {
            len,
            heap_len,
            timestamps,
            heap,
            data,
            types,
        }
    }

    pub fn columns(&self) -> &[SegmentArray] {
        bytes_to_columns(self.data)
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Segment {
    pub len: usize,
    pub heap_len: usize,
    pub timestamps: Box<[u64]>,
    pub heap: Box<[u8]>,
    pub data: Vec<u8>,
    pub types: Vec<FieldType>,
}

impl Segment {
    pub fn new_empty() -> Self {
        Segment {
            len: 0,
            heap_len: 0,
            timestamps: box [0; SEG_SZ],
            heap: box [0; HEAP_SZ],
            data: Vec::new(),
            types: Vec::new(),
        }
    }

    pub fn columns(&self) -> &[SegmentArray] {
        bytes_to_columns(self.data.as_slice())
    }

    pub fn columns_mut(&mut self) -> &mut [SegmentArray] {
        bytes_to_columns_mut(self.data.as_mut_slice())
    }

    pub fn set_fields(&mut self, types: &mut [FieldType]) {
        self.data.clear();
        self.data.resize(types.len() * 8 * 256, 0);
        self.types.clear();
        self.types.extend_from_slice(types);
    }

    pub fn clear_fields(&mut self) {
        self.data.clear();
        self.types.clear();
    }

    pub fn add_field(&mut self, field: FieldType) {
        let l = self.data.len();
        self.data.resize(l + 8 * 256, 0);
        self.types.push(field);
    }

    pub fn field_idx(&self, field: usize, idx: usize) -> SampleType {
        let cols = self.columns();
        let value = cols[field][idx];
        let field_type = self.types[field];
        match field_type {
            FieldType::Bytes => {
                SampleType::from_field_item(field_type, value, Some(&self.heap[..]))
            }
            _ => SampleType::from_field_item(field_type, value, None),
        }
    }

    pub fn row(&self, idx: usize, buffer: &mut Vec<SampleType>) {
        for i in 0..self.types.len() {
            buffer.push(self.field_idx(i, idx));
        }
    }
}
