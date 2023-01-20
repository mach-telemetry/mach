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

use crate::snapshot::Snapshot;
use crate::source::*;
use crate::writer::Writer;
use dashmap::DashMap;
use std::sync::Arc;
use crate::snapshotter::Snapshotter;

pub struct Mach {
    source_table: Arc<DashMap<SourceId, Source>>,
}

impl Mach {
    pub fn new() -> Self {
        Self {
            source_table: Arc::new(DashMap::new()),
        }
    }

    pub fn add_writer(&self) -> Writer {
        Writer::new(self.source_table.clone())
    }

    pub fn snapshot(&self, id: SourceId) -> Snapshot {
        let source = self.source_table.get(&id).unwrap().clone();
        source.snapshot()
    }

    pub fn snapshotter(&self) -> Snapshotter {
        Snapshotter::new(self.source_table.clone())
    }
}
