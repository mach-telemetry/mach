use std::{
    sync::{
        atomic::{
            AtomicUsize, Ordering::SeqCst
        }
    },
};
use crate::{
    active_buffer::{ActiveBuffer, ActiveBufferWriter},
    managed::Manager,
    buffer::{self, Buffer},
};

pub enum Error {
    Full
}

pub struct ActiveSegment {
    buffers: Vec<ActiveBuffer>
    buff_cnt: Arc<AtomicUsize>,
    has_writer: Arc<AtomicUsize>,
}

impl ActiveSegment {
    fn new(size: usize) -> Self {
        let manager: Manager<Buffer> = Manager::new();
        let buff_cnt = Arc::new(AtomicUsize::new(0));
        let has_writer = Arc::new(AtomicUsize::new(0));
        let mut buffers = Vec::new();
        for _ in 0..size {
            buffers.push(ActiveBuffer::new(manager.clone()));
        }
        ActiveSegment {
            buffers,
            buffer_id,
        }
    }

    fn writer(&self) -> ActiveSegmentWriter {
        if self.has_writer.fetch_add(1, SeqCst) > 1 {
            panic!("Multiple writers detected");
        }

        let cnt = self.buff_cnt.load(usize) % self.buffers.len();
        let buffers = self.buffers.map(|x| unsafe { x.writer() }).collect();
        let buffer = buffers[cnt]

        ActiveSegmentWriter {
            buffer: 
            buffers: self.buffers.map(|x| unsafe { x.writer() }).collect(),
            buff_cnt: self.buff_cnt.clone(),
            has_writer: self.has_writer.clone(),
        }
    }
}

pub struct ActiveSegmentWriter {
    buffer: * const Buffer,
    buffers: Vec<ActiveBufferWriter>,
    buff_cnt: Arc<AtomicUsize>,
    has_writer: Arc<AtomicUsize>
}

impl ActiveSegmentWriter {
    fn push(&mut self, ts: u64, data: &[[u8; 8]]) -> Result<(), Error> {
    }
}

pub struct ActiveSegmentReader {
    buffers: Vec<ManagedPtr<Buffer>>,
    buff_cnt: usize,
    has_writer: Arc<AtomicUsize>
}

