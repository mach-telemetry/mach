use crate::segment::{
    buffer::*,
    Error,
    PushStatus,
    FullSegment,
};
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
    local_head: usize, // always tracks the atomic head
    head: AtomicUsize,
    flushed: AtomicIsize,
    buffers: [Buffer<V>; B],
}

impl<const B: usize, const V: usize> Segment<B, V> {
    pub fn push(&mut self, ts: u64, item: &[[u8; 8]]) -> Result<PushStatus, Error> {
        match self.buffers[self.local_head % B].push(ts, item) {
            Ok(()) => Ok(PushStatus::Done),
            Err(Error::PushIntoFull) => {
                let can_move = self.local_head as isize - self.flushed.load(SeqCst) < B as isize;
                if can_move {
                    self.local_head = self.head.fetch_add(1, SeqCst) + 1;
                    let buf = &mut self.buffers[self.local_head % B];
                    buf.reuse(self.local_head);
                    buf.push(ts, item).unwrap();  // this must succeed
                    Ok(PushStatus::Flush)
                } else {
                    Err(Error::PushIntoFull)
                }
            }
            Err(x) => Err(x),
        }
    }

    pub fn to_flush(&self) -> Option<FullSegment> {
        let head = self.head.load(SeqCst);
        let to_flush = self.flushed.load(SeqCst) + 1;
        if head as isize > to_flush {
            let buf = &self.buffers[to_flush as usize % B];
            Some(buf.to_flush())
        } else {
            None
        }
    }

    pub fn flushed(&self) {
        self.flushed.fetch_add(1, SeqCst);
    }

    //pub fn flush<T: FlushSegmentTrait>(&self, flusher: &mut T) -> Result<(), Error> {
    //    let head = self.head.load(SeqCst);
    //    let to_flush = self.flushed.load(SeqCst) + 1;
    //    if head as isize > to_flush {
    //        let buf = &self.buffers[to_flush as usize % B];
    //        let mut d = [&[[0u8; 8]][..]; V];
    //        for i in 0..V {
    //            d[i] = &buf.data[i][..buf.len];
    //        }
    //        flusher.flush_segment(&buf.ts[..buf.len], &d)?;
    //        self.flushed.store(to_flush, SeqCst);
    //        Ok(())
    //    } else {
    //        Err(Error::FlushingHead)
    //    }
    //}

    //pub fn force_flush(&mut self, flusher: FlushFn) -> Result<(), Error> {
    //    let head = self.local_head;
    //    let to_flush = self.flushed.load(SeqCst) + 1;
    //    assert_eq!(head as isize, to_flush);
    //    let buf = &self.buffers[to_flush as usize % B];
    //    let mut d = [&[[0u8; 8]][..]; V];
    //    for i in 0..V {
    //        d[i] = &buf.data[i][..buf.len];
    //    }
    //    (flusher)(&buf.ts[..buf.len], &d)?;
    //    self.local_head = self.head.fetch_add(1, SeqCst) + 1;
    //    self.flushed.store(to_flush, SeqCst);
    //    Ok(())
    //}

    pub fn read(&self) -> Result<Vec<ReadBuffer>, Error> {
        let mut copies = Vec::new();
        for buf in self.buffers.iter() {
            let mut try_counter = 0;
            loop {
                if let Ok(x) = buf.read() {
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
        Ok(copies)
    }

    pub fn new() -> Self {
        let buffers = init_buffer_array();
        Segment {
            local_head: 0,
            head: AtomicUsize::new(0),
            flushed: AtomicIsize::new(-1),
            buffers,
        }
    }
}
