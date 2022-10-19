use std::mem::MaybeUninit;
use std::sync::{atomic::{AtomicUsize, Ordering::SeqCst}};
use std::cell::{UnsafeCell, RefCell};
use std::convert::TryInto;
use std::sync::{Arc, Mutex};
use crate::sample::{SampleType};


const BUFFER_SIZE: usize = 10_000_000_000;
const BUFFER_OVER: usize = 2_000_000;

struct BurstBuffer {
    bytes: [u8; BUFFER_SIZE + BUFFER_OVER],
    writer_offset: AtomicUsize,
    reader_offset: AtomicUsize,
}

pub fn init_burst_buffer() -> (BurstBufferWriter, BurstBufferReader) {
    let buffer = Arc::new(BurstBuffer {
        bytes: [0u8; BUFFER_SIZE + BUFFER_OVER],
        writer_offset: AtomicUsize::new(0),
        reader_offset: AtomicUsize::new(0),
    });
    (
        BurstBufferWriter { buffer: buffer.clone()},
        BurstBufferReader {
            buffer: buffer,
            first_read: RefCell::new(true),
        },
    )
}

pub struct BurstBufferWriter {
    buffer: Arc<BurstBuffer>
}

impl BurstBufferWriter {
    pub fn push_sample(&mut self, ser_id: u64, ref_id: u64, ts: u64, data: &[SampleType]) -> Result<(), ()> {
        let writer_offset = self.buffer.writer_offset.load(SeqCst);
        let reader_offset = self.buffer.reader_offset.load(SeqCst);
        let mut n_bytes: usize = data.iter().map(|x| x.byte_size()).sum();
        n_bytes += 32;
        let writer_idx_start = writer_offset % BUFFER_SIZE;
        let writer_idx_end = writer_idx_start + n_bytes;
        let reader_idx = reader_offset % BUFFER_SIZE;

        // Don't write if wrapped around and will exceed reader
        if writer_offset > 0 && writer_idx_start <= reader_idx && writer_idx_end > reader_idx {
            return Err(())
        }

        let burst_buffer = unsafe { Arc::get_mut_unchecked(&mut self.buffer) };
        let writer_offset = burst_buffer.writer_offset.load(SeqCst);
        let buffer = &mut burst_buffer.bytes[writer_idx_start..writer_idx_end];
        buffer[0..8].copy_from_slice(&n_bytes.to_be_bytes());
        buffer[8..16].copy_from_slice(&ser_id.to_be_bytes());
        buffer[16..24].copy_from_slice(&ref_id.to_be_bytes());
        buffer[24..32].copy_from_slice(&ts.to_be_bytes());
        let mut o = 32;
        for item in data.iter() {
            let dest = &mut buffer[o..];
            let sz = match item {
                SampleType::I64(x) => {
                    dest[0] = 0;
                    dest[1..9].copy_from_slice(&x.to_be_bytes());
                    9
                },
                SampleType::U64(x) => {
                    dest[0] = 1;
                    dest[1..9].copy_from_slice(&x.to_be_bytes());
                    9
                },
                SampleType::F64(x) => {
                    dest[0] = 2;
                    dest[1..9].copy_from_slice(&x.to_be_bytes());
                    9
                },
                SampleType::Timestamp(x) => {
                    dest[0] = 3;
                    dest[1..9].copy_from_slice(&x.to_be_bytes());
                    9
                },
                SampleType::Bytes(x) => {
                    dest[0] = 4;
                    let len = x.len();
                    dest[1..9].copy_from_slice(&len.to_be_bytes());
                    dest[9..len+9].copy_from_slice(x.as_slice());
                    len + 9
                },
            };
            o += sz;
        }
        assert_eq!(o, n_bytes);

        let n_bytes = if writer_idx_end < BUFFER_SIZE {
            n_bytes
        } else {
            BUFFER_SIZE - writer_idx_start
        };
        burst_buffer.writer_offset.fetch_add(n_bytes, SeqCst);
        Ok(())
    }
}

pub struct BurstBufferSample<'a> {
    pub ser_id: u64,
    pub ref_id: u64,
    pub timestamp: u64,
    pub items: Vec<BurstBufferType<'a>>
}

impl<'a> BurstBufferSample<'a> {
    pub fn new() -> Self {
        Self {
            ser_id: u64::MAX,
            ref_id: u64::MAX,
            timestamp: u64::MAX,
            items: Vec::new(),
        }
    }
}

pub enum BurstBufferType<'a> {
    I64(i64),
    F64(f64),
    U64(u64),
    Timestamp(u64),
    Bytes(&'a [u8]),
}

pub struct BurstBufferReader {
    buffer: Arc<BurstBuffer>,
    first_read: RefCell<bool>,
}

impl<'a> BurstBufferReader {

    pub fn next_sample(&self) -> Option<()> {
        let reader_offset = self.buffer.reader_offset.load(SeqCst);
        let writer_offset = self.buffer.writer_offset.load(SeqCst);
        let first_read = *self.first_read.borrow();

        let start_idx = reader_offset % BUFFER_SIZE;
        let buffer = &self.buffer.bytes[start_idx..];
        let n_bytes = usize::from_be_bytes(buffer[0..8].try_into().unwrap());

        let first_read = first_read && reader_offset == 0 && writer_offset > 0;
        let can_progress = reader_offset + n_bytes < writer_offset;

        if first_read {
            *self.first_read.borrow_mut() = false;
            Some(())
        } else if can_progress {
            let n_bytes = if start_idx + n_bytes < BUFFER_SIZE {
                n_bytes
            } else {
                BUFFER_SIZE - start_idx
            };
            self.buffer.reader_offset.fetch_add(n_bytes, SeqCst);
            Some(())
        } else {
            None
        }
    }

    pub fn read_sample(&'a self, burst_buffer_sample: &mut BurstBufferSample<'a>) {
        burst_buffer_sample.items.clear();
        let reader_offset = self.buffer.reader_offset.load(SeqCst);
        let start_idx = reader_offset % BUFFER_SIZE;
        let buffer = &self.buffer.bytes[start_idx..];
        let n_bytes = usize::from_be_bytes(buffer[0..8].try_into().unwrap());
        let ser_id = u64::from_be_bytes(buffer[8..16].try_into().unwrap());
        let ref_id = u64::from_be_bytes(buffer[16..24].try_into().unwrap());
        let timestamp = u64::from_be_bytes(buffer[24..32].try_into().unwrap());
        burst_buffer_sample.ser_id = ser_id;
        burst_buffer_sample.ref_id = ref_id;
        burst_buffer_sample.timestamp = timestamp;
        let mut o = 32;

        while o < n_bytes {
            match buffer[o] {
                0 => { 
                    let v = i64::from_be_bytes(buffer[o+1..o+9].try_into().unwrap());
                    let sample = BurstBufferType::I64(v);
                    burst_buffer_sample.items.push(sample);
                    o += 9;
                },
                1 => { 
                    let v = u64::from_be_bytes(buffer[o+1..o+9].try_into().unwrap());
                    let sample = BurstBufferType::U64(v);
                    burst_buffer_sample.items.push(sample);
                    o += 9;
                },
                2 => { 
                    let v = f64::from_be_bytes(buffer[o+1..o+9].try_into().unwrap());
                    let sample = BurstBufferType::F64(v);
                    burst_buffer_sample.items.push(sample);
                    o += 9;
                },
                3 => { 
                    let v = u64::from_be_bytes(buffer[o+1..o+9].try_into().unwrap());
                    let sample = BurstBufferType::Timestamp(v);
                    burst_buffer_sample.items.push(sample);
                    o += 9;
                },
                4 => { 
                    let len = usize::from_be_bytes(buffer[o+1..o+9].try_into().unwrap());
                    let (s, e) = (o + 9, o + 9 + len);
                    let sample = BurstBufferType::Bytes(&buffer[s..e]);
                    burst_buffer_sample.items.push(sample);
                    o += 9 + len;
                },
                _ => {
                    panic!("unidentified")
                }
            }
        }
    }


    //pub fn load_sample(&mut self) -> Option<()> {
    //    let writer_offset = self.buffer.writer_offset.load(SeqCst);
    //    if writer_offset > self.start {
    //    }
    //}

    //pub fn go_to_next_sample(&mut self) -> Option<()> {
    //    let writer_offset = self.buffer.writer_offset.load(SeqCst);
    //    let writer_epoch = self.buffer.writer_epoch.load(SeqCst);

    //    let next_start = {
    //        let next_start = self.start + self.size;
    //        if next_start < BUFFER_SIZE {
    //            next_start
    //        } else {
    //            0
    //        }
    //    };

    //    if (writer_epoch > self.reader_epoch) || (writer_epoch == self.reader_epoch && next_start < writer_offset) {
    //        self.buffer.reader_offset.store(next_start, SeqCst);
    //        self.buffer.reader_epoch.fetch_add((next_start > 0) as usize, SeqCst);
    //        self.start = next_start;
    //        // TODO read end
    //        self.end = 0;
    //        Some(())
    //    } else {
    //        None
    //    }
    //}
}

