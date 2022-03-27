use std::{
    sync::Arc,
};
use crate::{
    persistent_list2::active_block::{self, ActiveBlock, ActiveNode, StaticNode},
    durable_queue::{self, DurableQueueWriter},
    compression::{Compression, DecompressBuffer},
    constants::BUFSZ,
    id::SeriesId,
    segment::FullSegment,
    tags::Tags,
    utils::{
        byte_buffer::ByteBuffer,
        wp_lock::{NoDealloc, WpLock},
    },
};
use serde::*;

pub enum Error {
    ActiveBlock(active_block::Error),
    InconsistentHeadRead,
}

impl From<active_block::Error> for Error {
    fn from(item: active_block::Error) -> Self {
        Error::ActiveBlock(item)
    }
}


#[derive(Clone)]
pub struct List {
    head: Arc<WpLock<ActiveNode>>,
    active_block: Arc<WpLock<ActiveBlock>>,
}

impl List {
    pub fn writer(&self) -> ListWriter {
        ListWriter(self.clone())
    }

    pub fn snapshot(&self) -> Result<ListSnapshot, Error> {

        let guard = self.head.protected_read();
        let head = guard.static_node();
        if guard.release().is_err() {
            return Err(Error::InconsistentHeadRead);
        }

        // There are two cases: copied before flush and after flush
        if head.queue_offset == u64::MAX {
            // Safety: the read operation keeps track of its own version so we don't need the
            // version incremented by the WPLock
            let active_block = unsafe { self.active_block.unprotected_read() };
            let x = active_block.read(head)?;
            Ok(ListSnapshot {
                active_block: Some(x.0),
                next_node: x.1,
            })
        } else {
            Ok(ListSnapshot {
                active_block: None,
                next_node: head
            })
        }
    }
}

pub struct ListWriter(List);

impl ListWriter {
    pub fn push(
        &mut self,
        id: SeriesId,
        segment: &FullSegment,
        compression: &Compression,
        w: &mut DurableQueueWriter,
    ) -> Result<(), Error> {
        // SAFETY:
        // the head is modified only by this method,
        // push_bytes doesn't race with a concurrent ListReader.
        let (new_head, to_flush) = unsafe {
            let prev_node = self.0.head.unprotected_read().static_node();
            let buf = self.0.active_block.unprotected_write();
            let new_head =
                buf.push(id, segment, compression, prev_node);
            let to_flush = buf.is_full();
            (new_head, to_flush)
        };

        // Need to guard the writes now since we're updating the head
        let mut guard = self.0.head.protected_write();
        *guard = new_head;
        drop(guard);

        // Then now we check if we should flush. A concurrent reader can safely access the new
        // head. When making a copy of the buffer information, will fail if the buffer reset
        // concurrently.
        if to_flush {

            // Safety, the flush doesn't race with ListReader since the data the listreader will
            // access is always valid and will be correct
            unsafe { self.0.active_block.unprotected_write().flush(w)?; }

            // Now we guard because the listreader might be copying during this reset
            let mut guard = self.0.active_block.protected_write();
            guard.reset();
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ListSnapshot {
    active_block: Option<Vec<Box<[u8]>>>,
    next_node: StaticNode,
}

impl ListSnapshot {
}
