use std::mem::MaybeUninit;
use std::sync::{atomic::{AtomicUsize, Ordering::SeqCst}};
use std::cell::{UnsafeCell, RefCell};
use std::convert::TryInto;
use std::sync::{Arc, Mutex, RwLock};
use crate::sample::{SampleType};
use crate::utils::wp_lock::{NoDealloc, WpLock};
use crossbeam::channel::{bounded, Sender, Receiver};
use lazy_static::*;

const BUFFER_SIZE: usize = 10_000_000;
const BUFFER_OVER: usize = 2_000_000;

lazy_static! {
    static ref BUFFER_CHAN: Receiver<Box<[u8]>> = {
        let (tx, rx) = bounded(1);
        std::thread::spawn(move || {
            loop {
                let item = vec![0u8; BUFFER_SIZE + BUFFER_OVER].into_boxed_slice();
                tx.send(item).unwrap();
            }
        });
        rx
    };
}

struct Buffer {
    bytes: Box<[u8]>,
    len: usize,
    previous: Arc<RwLock<Option<Buffer>>>,
}

struct Head {
    bytes: Box<[u8]>,
    len: AtomicUsize,
    previous: Arc<RwLock<Arc<RwLock<Option<Buffer>>>>>
}

impl Head {
    fn push_sample(&mut self, ser_id: u64, ref_id: u64, ts: u64, data: &[SampleType]) -> bool {
        let writer_offset = self.len.load(SeqCst);
        let buffer = &mut self.bytes[writer_offset..];
        buffer[0..8].copy_from_slice(&0usize.to_be_bytes());
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
        buffer[0..8].copy_from_slice(&o.to_be_bytes());
        let total = self.len.fetch_add(o, SeqCst) + o;
        total > BUFFER_SIZE
    }

    fn next_epoch(&mut self) -> Arc<RwLock<Option<Buffer>>> {
        let len = self.len.swap(0, SeqCst);
        let mut bytes = BUFFER_CHAN.recv().unwrap();
        std::mem::swap(&mut bytes, &mut self.bytes);
        let previous = self.previous.read().unwrap().clone();
        let previous = Arc::new(RwLock::new(Some(Buffer {
            bytes,
            len,
            previous,
        })));
        *self.previous.write().unwrap() = previous.clone();
        previous
    }

    fn read(&self) -> Buffer {
        let len = self.len.load(SeqCst);
        let bytes = self.bytes[..len].into();
        let previous = self.previous.read().unwrap().clone();
        Buffer {
            bytes,
            len,
            previous
        }
    }
}

unsafe impl NoDealloc for Head { }

pub fn burst_buffer() -> BurstBufferWriter {
    let head = Head {
        bytes: BUFFER_CHAN.recv().unwrap(),
        len: AtomicUsize::new(0),
        previous: Arc::new(RwLock::new(Arc::new(RwLock::new(None)))),
    };

    BurstBufferWriter {
        head: Arc::new(WpLock::new(head)),
        vec: Vec::with_capacity(1_000_000),
    }
}

pub struct BurstBufferWriter {
    head: Arc<WpLock<Head>>,
    vec: Vec<Arc<RwLock<Option<Buffer>>>>
}

impl BurstBufferWriter {
    pub fn push_sample(&mut self, ser_id: u64, ref_id: u64, ts: u64, data: &[SampleType]) {
        let head = unsafe { self.head.unprotected_write() };
        if head.push_sample(ser_id, ref_id, ts, data) {
            drop(head);
            let buff = self.head.protected_write().next_epoch();
            self.vec.push(buff);
        }
    }
}
