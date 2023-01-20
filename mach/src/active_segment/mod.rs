use crate::{
    constants::{HEAP_SZ, SEG_SZ},
    field_type::FieldType,
    sample::SampleType,
    segment::{Segment, SegmentRef},
};
use std::cell::UnsafeCell;
use std::sync::{
    atomic::{AtomicUsize, AtomicBool, Ordering::SeqCst},
    Arc,
};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum PushStatus {
    Full,
    Ok,
    ErrorFull,
}

impl PushStatus {
    #[inline]
    pub fn is_error_full(self) -> bool {
        self == PushStatus::ErrorFull
    }

    #[inline]
    pub fn is_full(self) -> bool {
        self == PushStatus::Full
    }

    #[inline]
    pub fn is_ok(self) -> bool {
        self == PushStatus::Ok
    }
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
        data + HEAP_SZ * 2
    } else {
        data
    }
}

struct Inner {
    len: usize,
    heap_len: usize,
    atomic_full: AtomicBool,
    atomic_len: AtomicUsize,
    atomic_heap_len: AtomicUsize,
    ts: [u64; SEG_SZ],
    data: Box<[u8]>,
    types: Vec<FieldType>,
    heap_offset: usize,
}

impl Inner {
    fn new(types: &[FieldType]) -> Self {
        let data = vec![0u8; data_size(types)].into_boxed_slice();
        let types: Vec<FieldType> = types.into();
        let heap_offset = {
            let colsz = 8 * SEG_SZ;
            colsz * types.len()
        };
        Inner {
            len: 0,
            heap_len: 0,
            atomic_full: AtomicBool::new(false),
            atomic_len: AtomicUsize::new(0),
            atomic_heap_len: AtomicUsize::new(0),
            ts: [0u64; SEG_SZ],
            data,
            types,
            heap_offset
        }
    }

    fn reset(&mut self) {
        self.len = 0;
        self.heap_len = 0;
        self.atomic_len.store(0, SeqCst);
        self.atomic_full.store(false, SeqCst);
    }

    #[inline]
    fn field_offsets(&self, idx: usize) -> (usize, usize) {
        let colsz = 8 * SEG_SZ;
        let start = colsz * idx;
        let end = start + colsz;
        (start, end)
    }

    //#[inline]
    //fn heap_offset(&self) -> usize {
    //    let colsz = 8 * SEG_SZ;
    //    colsz * self.types.len()
    //}

    fn push(&mut self, ts: u64, items: &[SampleType]) -> PushStatus {
        if self.atomic_full.load(SeqCst) {
            return PushStatus::ErrorFull
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
                    //let heap_start = self.heap_offset();
                    //self.data[offset..offset_end].copy_from_slice(&self.heap_len.to_be_bytes());
                    let colsz = 8 * SEG_SZ;
                    let heap_offset = colsz * self.types.len();
                    let heap: &mut [u8] = &mut self.data[heap_offset..];
                    let heap_start = self.heap_len;
                    let mut heap_off = self.heap_len;

                    let bytes_len = b.len();
                    heap[heap_off..heap_off + 8].copy_from_slice(&bytes_len.to_be_bytes());
                    heap_off += 8;
                    heap[heap_off..heap_off + bytes_len].copy_from_slice(b.as_slice());
                    heap_off += bytes_len;

                    self.heap_len = heap_off;
                    //self.atomic_heap_len.fetch_add(heap_off, SeqCst);
                    //self.data[offset..offset_end].copy_from_slice(&heap_start.to_be_bytes());
                }
            }
        }
        self.len += 1;

        // Linearize at this point
        self.atomic_len.fetch_add(1, SeqCst);

        if self.len == SEG_SZ || self.heap_len > HEAP_SZ {
            self.atomic_full.store(true, SeqCst);
            PushStatus::Full
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
            heap,
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
pub struct ActiveSegmentWriter {
    segment: Arc<InnerActiveSegment>,
}

impl ActiveSegmentWriter {
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

// Safety: ActiveSegmentWriter syncs with readers because &mut methods require it's the only
// writer. Synchronization implemented Inner and the restricted methods of ActiveSegment
unsafe impl Sync for ActiveSegmentWriter {}
unsafe impl Send for ActiveSegmentWriter {}

#[derive(Clone)]
pub struct ActiveSegment {
    segment: Arc<InnerActiveSegment>,
}

impl ActiveSegment {
    pub fn new(types: &[FieldType]) -> (Self, ActiveSegmentWriter) {
        let segment = Arc::new(InnerActiveSegment::new(types));
        let this = Self {
            segment: segment.clone(),
        };
        let writer = ActiveSegmentWriter { segment };
        (this, writer)
    }

    pub unsafe fn reset(&self) {
        self.segment.reset();
    }

    pub fn as_segment_ref(&self) -> SegmentRef {
        // Safety: Because there is only ever one writer, this is safe
        unsafe { self.segment.as_segment_ref() }
    }

    pub fn snapshot(&self) -> Result<Segment, &'static str> {
        self.segment.snapshot()
    }
}

// Safety: It is safe to share ActiveSegment with multiple threads because of the sync mechansims
// implemented in Inner and the restricted methods of ActiveSegment. See Above.
unsafe impl Sync for ActiveSegment {}
unsafe impl Send for ActiveSegment {}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::*;

    #[test]
    fn test() {
        let types = &[FieldType::Bytes, FieldType::F64];
        let samples = random_samples(types, SEG_SZ, 16..1024);
        let expected_floats = &samples[1];
        let expected_strings = &samples[0];

        let (active_segment, mut writer) = ActiveSegment::new(types);

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
            PushStatus::Full
        );
        assert_eq!(
            writer.push(SEG_SZ as u64 + 1, values.as_slice()),
            PushStatus::ErrorFull
        );

        let seg = active_segment.snapshot().unwrap();

        let strings: Vec<SampleType> = (0..SEG_SZ).map(|x| seg.field_idx(0, x)).collect();
        let floats: Vec<SampleType> = (0..SEG_SZ).map(|x| seg.field_idx(1, x)).collect();
        assert_eq!(floats.as_slice(), expected_floats.as_slice());
        assert_eq!(strings.as_slice(), expected_strings.as_slice());
    }
}
