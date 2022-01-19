use crate::constants::*;
use crate::segment::{buffer::*, Error, FullSegment, InnerPushStatus};
use std::{
    mem,
    sync::atomic::{AtomicIsize, AtomicUsize, Ordering::SeqCst},
};

fn init_buffer_array<const B: usize, const V: usize>() -> [Buffer<V>; B] {
    let mut buffers: [mem::MaybeUninit<Buffer<V>>; B] = mem::MaybeUninit::uninit_array();
    for b in buffers.iter_mut() {
        b.write(Buffer::<V>::new());
    }
    unsafe { mem::MaybeUninit::array_assume_init(buffers) }
}

pub struct Segment<const B: usize, const V: usize> {
    local_count: usize,
    local_head: usize, // always tracks the atomic head
    head: AtomicUsize,
    flushed: AtomicIsize,
    buffers: [Buffer<V>; B],
}

impl<const B: usize, const V: usize> Segment<B, V> {
    pub fn push_univariate(&mut self, ts: u64, item: [u8; 8]) -> Result<InnerPushStatus, Error> {
        if self.local_count == SEGSZ {
            self.try_next_buffer();
        }
        let res = self.current_buffer().push_univariate(ts, item);
        if res.is_ok() {
            self.local_count += 1;
        }
        res
    }

    pub fn push_item(
        &mut self,
        ts: u64,
        item: [[u8; 8]; V],
    ) -> Result<InnerPushStatus, Error> {
        if self.local_count == SEGSZ {
            self.try_next_buffer();
        }
        let res = self.current_buffer().push_item(ts, item);
        if res.is_ok() {
            self.local_count += 1;
        }
        res
    }

    pub fn push(&mut self, ts: u64, item: &[[u8; 8]]) -> Result<InnerPushStatus, Error> {
        if self.local_count == SEGSZ {
            self.try_next_buffer();
        }
        let res = self.current_buffer().push(ts, item);
        if res.is_ok() {
            self.local_count += 1;
        }
        res
    }

    #[inline]
    fn current_buffer(&mut self) -> &mut Buffer<V> {
        &mut self.buffers[self.local_head % B]
    }

    fn try_next_buffer(&mut self) -> bool {
        let flushed = self.flushed.load(SeqCst);
        //println!("local: {}, flushed: {}, B: {}", self.local_head, flushed, B);
        if self.local_head as isize - flushed < B as isize {
            //println!("HERE");
            self.local_head = self.head.fetch_add(1, SeqCst) + 1;
            let buf = &mut self.buffers[self.local_head % B];
            buf.reset();
            self.local_count = 0;
            true
        } else {
            false
        }
    }

    pub fn to_flush(&self) -> Option<FullSegment> {
        let head = self.head.load(SeqCst);
        let to_flush = self.flushed.load(SeqCst) + 1;
        if head as isize >= to_flush {
            let buf = &self.buffers[to_flush as usize % B];
            buf.to_flush()
        } else {
            None
        }
    }

    pub fn flushed(&self) {
        self.flushed.fetch_add(1, SeqCst);
    }

    pub fn read(&self) -> Result<Vec<ReadBuffer>, Error> {
        let mut copies = Vec::new();
        for buf in self.buffers.iter() {
            let mut try_counter = 0;
            loop {
                if let Some(x) = buf.read() {
                    copies.push(x);
                    break;
                } else {
                    try_counter += 1;
                    if try_counter == 3 {
                        return Err(Error::InconsistentCopy);
                    }
                }
            }
        }

        // Make sure newest buffer is first
        use std::cmp::Reverse;
        copies.sort_by_key(|x| {
            if x.len() > 0 {
                Reverse(x.timestamps()[0])
            } else {
                Reverse(u64::MAX)
            }
        });

        Ok(copies)
    }

    pub fn new() -> Self {
        let buffers = init_buffer_array();
        Segment {
            local_count: 0,
            local_head: 0,
            head: AtomicUsize::new(0),
            flushed: AtomicIsize::new(-1),
            buffers,
        }
    }
}
