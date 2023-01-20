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

use crate::{
    active_block::ActiveBlock, active_segment::ActiveSegment, compression::Compression,
    field_type::FieldType, mem_list::metadata_list::MetadataList, snapshot::Snapshot,
};
use serde::*;
use std::ops::Deref;

#[derive(Copy, Clone, Eq, Debug, PartialEq, Hash, Serialize, Deserialize)]
pub struct SourceId(pub u64);

impl Deref for SourceId {
    type Target = u64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<u64> for SourceId {
    fn from(id: u64) -> Self {
        SourceId(id as u64)
    }
}

#[derive(Clone)]
pub struct SourceConfig {
    pub id: SourceId,
    pub types: Vec<FieldType>,
    pub compression: Compression,
}

//pub struct SourceWriter {
//    config: SourceConfig,
//    active_segment: ActiveSegmentWriter,
//    active_block: ActiveBlockWriter,
//}

//impl SourceWriter {
//    pub fn push(&mut self, ts: u64, items: &[SampleType]) {
//        match self.active_segment.push(ts, items) {
//            active_segment::PushStatus::Full => {
//                let segment_reference = self.active_segment.as_segment_ref();
//                let id = self.config.id;
//                let compression = &self.config.compression;
//                self.active_block.push(id, segment_reference, compression);
//                self.active_segment.reset();
//            }
//            active_segment::PushStatus::Ok => {},
//            active_segment::PushStatus::ErrorFull => unreachable!(),
//        }
//    }
//}

//unsafe impl Sync for Source{}
//unsafe impl Send for Source{}

#[derive(Clone)]
pub struct Source {
    pub config: SourceConfig,
    pub active_segment: ActiveSegment,
    pub active_block: ActiveBlock,
    pub metadata_list: MetadataList,
}

impl Source {
    pub fn snapshot(&self) -> Snapshot {
        let active_segment = self.active_segment.snapshot().ok();
        let active_block = self.active_block.read_only().ok();
        let metadata_list = {
            let mut metadata_list = self.metadata_list.read();
            loop {
                if metadata_list.is_ok() {
                    break;
                } else {
                    metadata_list = self.metadata_list.read();
                }
            }
            metadata_list.unwrap()
        };

        Snapshot {
            source_id: self.config.id,
            active_segment,
            active_block,
            metadata_list,
        }
    }
}
