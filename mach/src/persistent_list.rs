use crate::{
    active_block::{self, ActiveBlock, ActiveNode, StaticNode},
    compression::Compression,
    durable_queue::{DurableQueueReader, DurableQueueWriter},
    id::SeriesId,
    segment::FlushSegment,
    utils::wp_lock::WpLock,
};
use serde::*;
use std::sync::Arc;

#[derive(Debug)]
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
    pub fn new(active_block: Arc<WpLock<ActiveBlock>>) -> Self {
        Self {
            head: Arc::new(WpLock::new(ActiveNode::new())),
            active_block,
        }
    }

    pub fn writer(&self) -> ListWriter {
        ListWriter(self.clone())
    }

    pub fn snapshot(&self) -> Result<ListSnapshot, Error> {
        let guard = self.head.protected_read();
        let head = guard.static_node();
        if guard.release().is_err() {
            return Err(Error::InconsistentHeadRead);
        }

        // If the head block version is usize::MAX, there's nothing coming after
        if head.block_version == usize::MAX {
            return Ok(ListSnapshot {
                active_block: None,
                next_node: head,
            })
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
                next_node: head,
            })
        }
    }
}

pub struct ListWriter(List);

impl ListWriter {
    pub async fn push(
        &mut self,
        id: SeriesId,
        segment: FlushSegment,
        compression: &Compression,
        w: &mut DurableQueueWriter,
    ) -> Result<(), Error> {
        let full_segment = segment.to_flush().unwrap();
        // SAFETY:
        // the head is modified only by this method,
        // push_bytes doesn't race with a concurrent ListReader.
        let (new_head, to_flush) = unsafe {
            let prev_node = self.0.head.unprotected_read().static_node();
            let buf = self.0.active_block.unprotected_write();
            let new_head = buf.push(id, &full_segment, compression, prev_node);
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
            unsafe {
                self.0.active_block.unprotected_write().flush(w).await?;
            }

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
    pub fn reader(&self, durable_queue: DurableQueueReader) -> Result<ListSnapshotReader, Error> {
        ListSnapshotReader::new(self, durable_queue)
    }
}

pub struct ListSnapshotReader {
    block: Vec<Box<[u8]>>,
    block_idx: usize,
    next_node: StaticNode,
    durable_queue: DurableQueueReader,
}

impl ListSnapshotReader {
    pub fn new(
        list_snapshot: &ListSnapshot,
        mut durable_queue: DurableQueueReader,
    ) -> Result<Self, Error> {
        if list_snapshot.active_block.is_some() {
            let block = list_snapshot.active_block.as_ref().unwrap().clone();
            Ok(ListSnapshotReader {
                block,
                block_idx: 0,
                next_node: list_snapshot.next_node,
                durable_queue,
            })
        } else {
            let (block, next_node) = list_snapshot
                .next_node
                .read_from_queue(&mut durable_queue)?;
            Ok(ListSnapshotReader {
                block,
                block_idx: 0,
                next_node,
                durable_queue,
            })
        }
    }

    pub fn next_item(&mut self) -> Result<Option<&[u8]>, Error> {
        if self.block_idx < self.block.len() {
            let idx = self.block_idx;
            self.block_idx += 1;
            Ok(Some(&self.block[idx]))
        } else {
            let (block, next_node) = self.next_node.read_from_queue(&mut self.durable_queue)?;
            //println!("NEXT NODE: {:?}", next_node);
            self.block = block;
            self.next_node = next_node;
            self.block_idx = 1;
            Ok(Some(&self.block[0]))
        }
    }
}
