use crate::constants::*;
use crate::sample::Type;
use crate::segment::{buffer::*, Error};
use std::{
    mem,
    sync::atomic::{AtomicIsize, AtomicUsize, Ordering::SeqCst},
};

pub struct Segment {
    local_head: usize, // always tracks the atomic head
    head: AtomicUsize,
    flushed: AtomicIsize,
    current_buffer: *mut Buffer,
    buffers: Vec<Buffer>,
}

impl Segment {
    pub fn push_item(&mut self, ts: u64, item: &[[u8; 8]]) -> Result<InnerPushStatus, Error> {
        let mut buf = unsafe { self.current_buffer.as_mut().unwrap() };
        let res = buf.push_item(ts, item);
        match res {
            Err(Error::PushIntoFull) => {
                self.try_next_buffer();
                buf = unsafe { self.current_buffer.as_mut().unwrap() };
                buf.push_item(ts, item)
            }
            Err(x) => Err(x),
            Ok(x) => Ok(x),
        }
    }

    pub fn push_type(&mut self, ts: u64, item: &[Type]) -> Result<InnerPushStatus, Error> {
        let mut buf = unsafe { self.current_buffer.as_mut().unwrap() };
        let res = buf.push_type(ts, item);
        match res {
            Err(Error::PushIntoFull) => {
                self.try_next_buffer();
                buf = unsafe { self.current_buffer.as_mut().unwrap() };
                buf.push_type(ts, item)
            }
            Err(x) => Err(x),
            Ok(x) => Ok(x),
        }
    }

    #[inline]
    fn current_buffer(&mut self) -> &mut Buffer {
        let b = self.buffers.len();
        &mut self.buffers[self.local_head % b]
    }

    fn try_next_buffer(&mut self) -> bool {
        let b = self.buffers.len();
        let flushed = self.flushed.load(SeqCst);
        if self.local_head as isize - flushed < self.buffers.len() as isize {
            self.local_head = self.head.fetch_add(1, SeqCst) + 1;
            // reset the new current buffer
            let buf = &mut self.buffers[self.local_head % b];
            buf.reset();
            self.current_buffer = buf;
            true
        } else {
            false
        }
    }

    pub fn to_flush(&self) -> Option<FlushBuffer> {
        let head = self.head.load(SeqCst);
        let to_flush = self.flushed.load(SeqCst) + 1;
        if head as isize >= to_flush {
            let buf = &self.buffers[to_flush as usize % self.buffers.len()];
            buf.to_flush()
        } else {
            println!("SEGMENT NONE");
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

    pub fn new(nbuffers: usize, heap_pointers: &[bool]) -> Self {
        let mut buffers: Vec<Buffer> = (0..nbuffers).map(|_| Buffer::new(heap_pointers)).collect();
        Segment {
            local_head: 0,
            head: AtomicUsize::new(0),
            flushed: AtomicIsize::new(-1),
            current_buffer: &mut buffers[0],
            buffers,
        }
    }
}
