use crate::{
    constants::{HEAP_SZ, HEAP_TH, SEG_SZ},
    field_type::FieldType,
    sample::SampleType,
    segment::{Segment, SegmentRef},
};
use std::cell::UnsafeCell;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
    Arc,
};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum PushStatus {
    IsFull,
    Ok,
    ErrorFull,
}

fn data_size(types: &[FieldType]) -> usize {
    let data = 8 * SEG_SZ * types.len();
    let mut has_heap = false;
    for t in types {
        if t == &FieldType::Bytes {
            has_heap = true;
            break;
        }
    }

    if has_heap {
        data + HEAP_SZ
    } else {
        data
    }
}

struct Inner {
    len: usize,
    heap_len: usize,
    atomic_len: AtomicUsize,
    atomic_heap_len: AtomicUsize,
    ts: [u64; SEG_SZ],
    data: Box<[u8]>,
    types: Vec<FieldType>,
}

impl Inner {
    fn new(types: &[FieldType]) -> Self {
        let data = vec![0u8; data_size(types)].into_boxed_slice();
        let types: Vec<FieldType> = types.into();
        Inner {
            len: 0,
            heap_len: 0,
            atomic_len: AtomicUsize::new(0),
            atomic_heap_len: AtomicUsize::new(0),
            ts: [0u64; SEG_SZ],
            data,
            types,
        }
    }

    fn reset(&mut self) {
        self.len = 0;
        self.heap_len = 0;
        self.atomic_len.store(0, SeqCst);
    }

    #[inline]
    fn field_offsets(&self, idx: usize) -> (usize, usize) {
        let colsz = 8 * SEG_SZ;
        let start = colsz * idx;
        let end = start + colsz;
        (start, end)
    }

    #[inline]
    fn heap_offset(&self) -> usize {
        let colsz = 8 * SEG_SZ;
        colsz * self.types.len()
    }

    fn push(&mut self, ts: u64, items: &[SampleType]) -> PushStatus {
        if self.len == SEG_SZ {
            return PushStatus::ErrorFull;
        }

        let len = self.len;
        self.ts[len] = ts;
        for (i, item) in items.iter().enumerate() {
            let (offset, offset_end) = {
                let (s, _) = self.field_offsets(i);
                let s = s + 8 * self.len;
                let e = s + 8;
                (s, e)
            };
            match item {
                SampleType::F64(x) => {
                    self.data[offset..offset_end].copy_from_slice(&x.to_be_bytes())
                }
                SampleType::I64(x) => {
                    self.data[offset..offset_end].copy_from_slice(&x.to_be_bytes())
                }
                SampleType::U64(x) => {
                    self.data[offset..offset_end].copy_from_slice(&x.to_be_bytes())
                }
                SampleType::Timestamp(x) => {
                    self.data[offset..offset_end].copy_from_slice(&x.to_be_bytes())
                }
                SampleType::Bytes(b) => {
                    let heap_start = self.heap_offset();
                    let heap: &mut [u8] = &mut self.data[heap_start..];
                    let heap_start = self.heap_len;
                    let mut heap_off = self.heap_len;

                    let bytes_len = b.len();
                    heap[heap_off..heap_off + 8].copy_from_slice(&bytes_len.to_be_bytes());
                    heap_off += 8;
                    heap[heap_off..heap_off + bytes_len].copy_from_slice(b.as_slice());
                    heap_off += bytes_len;

                    self.heap_len = heap_off;
                    self.atomic_heap_len.fetch_add(heap_off, SeqCst);
                    self.data[offset..offset_end].copy_from_slice(&heap_start.to_be_bytes());
                }
            }
        }
        self.len += 1;

        // Linearize at this point
        self.atomic_len.fetch_add(1, SeqCst);

        if self.len == SEG_SZ || self.heap_len == HEAP_TH {
            PushStatus::IsFull
        } else {
            PushStatus::Ok
        }
    }

    fn as_segment_ref(&self) -> SegmentRef {
        let len = self.atomic_len.load(SeqCst);
        let heap_len = self.heap_len;
        let data = &self.data[..8 * self.types.len() * SEG_SZ];
        let heap = &self.data[8 * self.types.len() * SEG_SZ..];
        let s = SegmentRef {
            len,
            heap_len,
            timestamps: &self.ts,
            heap: heap.try_into().unwrap(),
            data,
            types: self.types.as_slice(),
        };
        s

    }
}

struct InnerActiveSegment {
    version: AtomicUsize,
    inner: UnsafeCell<Inner>,
}

impl InnerActiveSegment {
    fn new(types: &[FieldType]) -> Self {
        InnerActiveSegment {
            version: AtomicUsize::new(0),
            inner: UnsafeCell::new(Inner::new(types)),
        }
    }

    /// Safety: This method is unsafe if there are exists a concurrent writer (e.g., push, reset)
    unsafe fn as_segment_ref(&self) -> SegmentRef {
        (*self.inner.get()).as_segment_ref()
    }

    fn push(&self, ts: u64, items: &[SampleType]) -> PushStatus {
        unsafe { (*self.inner.get()).push(ts, items) }
    }

    fn reset(&self) {
        self.version.fetch_add(1, SeqCst);
        unsafe { (*self.inner.get()).reset() }
        self.version.fetch_add(1, SeqCst);
    }

    fn snapshot(&self) -> Result<Segment, &'static str> {
        let version = self.version.load(SeqCst);
        // Safety: This is safe because if the counter cannot be compared, data in the segment is
        // potentially erroneous and return an error
        let seg = unsafe { self.as_segment_ref() }.to_segment();
        if version != self.version.load(SeqCst) {
            Err("Failed to make segment snapshot")
        } else {
            Ok(seg)
        }
    }
}

// This should never be Clone - there can only exist one writer for each segment ever
pub struct WriteActiveSegment {
    has_writer: Arc<AtomicBool>,
    segment: Arc<InnerActiveSegment>,
}

impl WriteActiveSegment {
    pub fn push(&mut self, ts: u64, items: &[SampleType]) -> PushStatus {
        self.segment.push(ts, items)
    }

    pub fn reset(&mut self) {
        self.segment.reset();
    }

    pub fn as_segment_ref(&self) -> SegmentRef {
        // Safety: Because there is only ever one writer, this is safe
        unsafe { self.segment.as_segment_ref() }
    }
}

impl Drop for WriteActiveSegment {
    fn drop(&mut self) {
        self.has_writer
            .compare_exchange(true, false, SeqCst, SeqCst)
            .unwrap();
    }
}

#[derive(Clone)]
pub struct ActiveSegment {
    has_writer: Arc<AtomicBool>,
    segment: Arc<InnerActiveSegment>,
}

impl ActiveSegment {
    pub fn writer(&self) -> WriteActiveSegment {
        self.has_writer
            .compare_exchange(false, true, SeqCst, SeqCst)
            .unwrap();
        WriteActiveSegment {
            has_writer: self.has_writer.clone(),
            segment: self.segment.clone(),
        }
    }

    pub fn new(types: &[FieldType]) -> Self {
        let segment = Arc::new(InnerActiveSegment::new(types));
        let has_writer = Arc::new(AtomicBool::new(false));
        Self {
            segment,
            has_writer,
        }
    }

    pub fn snapshot(&self) -> Result<Segment, &'static str> {
        self.segment.snapshot()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use rand::{
        distributions::{Alphanumeric, DistString},
        thread_rng, Rng,
    };

    #[test]
    fn test() {
        let mut rng = thread_rng();
        let expected_floats: Vec<SampleType> =
            (0..SEG_SZ).map(|_| SampleType::F64(rng.gen())).collect();
        let expected_strings: Vec<SampleType> = (0..SEG_SZ)
            .map(|_| {
                let string = Alphanumeric.sample_string(&mut rng, 16);
                SampleType::Bytes(string.into_bytes())
            })
            .collect();

        let types = &[FieldType::Bytes, FieldType::F64];
        let active_segment = ActiveSegment::new(types);
        let mut writer = active_segment.writer();

        let mut values = Vec::new();
        for i in 0..SEG_SZ - 1 {
            let a = expected_strings[i].clone();
            let b = expected_floats[i].clone();
            values.push(a);
            values.push(b);
            assert_eq!(writer.push(i as u64, values.as_slice()), PushStatus::Ok);
            values.clear();
        }
        let a = expected_strings[SEG_SZ - 1].clone();
        let b = expected_floats[SEG_SZ - 1].clone();
        values.push(a);
        values.push(b);
        assert_eq!(
            writer.push(SEG_SZ as u64, values.as_slice()),
            PushStatus::IsFull
        );
        assert_eq!(
            writer.push(SEG_SZ as u64 + 1, values.as_slice()),
            PushStatus::ErrorFull
        );

        let seg = active_segment.snapshot().unwrap();

        let strings: Vec<SampleType> = (0..SEG_SZ).map(|x| seg.field_idx(0, x)).collect();
        let floats: Vec<SampleType> = (0..SEG_SZ).map(|x| seg.field_idx(1, x)).collect();
        assert_eq!(floats, expected_floats);
        assert_eq!(strings, expected_strings);
    }
}
