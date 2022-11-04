use crate::snapshot::Snapshot;
use crate::source::*;
use crate::writer::Writer;
use dashmap::DashMap;
use std::sync::Arc;

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
}
