// Copyright (c) 2023 Franco Solleza, Intel Corporation, Brown University
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

use crate::active_block::{BlockMetadata, ReadOnlyBlock};
use crate::mem_list::read_only::ReadOnlyMetadataBlock;
use crate::segment::Segment;
use crate::source::SourceId;
use serde::*;

#[derive(Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub source_id: SourceId,
    pub active_segment: Option<Segment>,
    pub active_block: Option<ReadOnlyBlock>,
    pub metadata_list: ReadOnlyMetadataBlock,
}

impl Snapshot {
    pub fn into_snapshot_iterator(self) -> SnapshotIterator {
        let source_id = self.source_id;
        let active_segment = self.active_segment;
        match self.active_block {
            Some(block) => {
                let block_metadata: Vec<BlockMetadata> = block
                    .get_metadata()
                    .iter()
                    .filter(|x| x.source_id == source_id)
                    .copied()
                    .collect();
                let block_list_idx = block_metadata.len();
                let list = self.metadata_list;
                let list_idx = list.len();
                SnapshotIterator {
                    source_id,
                    active_segment,
                    block,
                    block_metadata,
                    block_list_idx,
                    list,
                    list_idx,
                }
            }

            None => {
                let mut list = self.metadata_list;
                let list_idx = list.len() - 1;
                let block = ReadOnlyBlock::from_closed_bytes(list.get_data_bytes(list_idx));
                let block_metadata: Vec<BlockMetadata> = block
                    .get_metadata()
                    .iter()
                    .filter(|x| x.source_id == source_id)
                    .copied()
                    .collect();
                let block_list_idx = block_metadata.len();
                SnapshotIterator {
                    source_id,
                    active_segment,
                    block,
                    block_metadata,
                    block_list_idx,
                    list,
                    list_idx,
                }
            }
        }
    }
}

pub struct SnapshotIterator {
    source_id: SourceId,

    active_segment: Option<Segment>,

    // Everything else is blocks, potentially starting with the active block
    block: ReadOnlyBlock,
    block_metadata: Vec<BlockMetadata>,
    block_list_idx: usize,
    list: ReadOnlyMetadataBlock,
    list_idx: usize,
}

impl SnapshotIterator {
    fn load_next_metadata_block(&mut self) -> Option<()> {
        self.list = self.list.load_next()?;
        self.list_idx = self.list.len();
        Some(())
    }

    fn load_next_block(&mut self) -> Option<()> {
        if self.list_idx == 0 {
            self.load_next_metadata_block()?;
        }
        self.list_idx -= 1;
        self.block = ReadOnlyBlock::from_closed_bytes(self.list.get_data_bytes(self.list_idx));
        self.block_metadata = self
            .block
            .get_metadata()
            .iter()
            .filter(|x| x.source_id == self.source_id)
            .copied()
            .collect();
        self.block_list_idx = self.block_metadata.len();
        Some(())
    }

    pub fn next_segment(&mut self) -> Option<Segment> {
        if self.active_segment.is_none() {
            if self.block_list_idx == 0 {
                self.load_next_block()?;
            }
            self.block_list_idx -= 1;
            Some(
                self.block
                    .get_segment(self.block_metadata[self.block_list_idx]),
            )
        } else {
            self.active_segment.take()
        }
    }
}
