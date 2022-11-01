use crate::source::*;
use crate::writer::{WriterId, WriterMetadata};
use crate::mem_list::metadata_list::MetadataList;
use crate::active_segment::ActiveSegment;
use rand::prelude::{thread_rng, SliceRandom};
use std::sync::Arc;
use dashmap::DashMap;
use lazy_static::lazy_static;

pub struct Mach {
    sources: Arc<DashMap<SourceId, Source>>,
    writer_table: Arc<DashMap<WriterId, WriterMetadata>>,
    writers: Vec<WriterId>,
}

impl Mach {
    pub fn add_source(&self, config: SourceConfig) -> WriterId {
        let source_id = config.id;
        let (metadata_list, metadata_list_writer) = MetadataList::new();
        let (active_segment, active_segment_writer) = ActiveSegment::new(config.types.as_slice());
        //assert!(self.source_writers.insert(config.id, (active_segment_writer, metadata_list_writer)).is_none());

        let writer_id = *self.writers.choose(&mut thread_rng()).unwrap();
        let active_block = self.writer_table.get(&writer_id).unwrap().active_block.clone();
        active_block.add_source(source_id, metadata_list_writer);

        let source = Source {
            config,
            active_segment,
            metadata_list,
            active_block,
        };
        self.sources.insert(source_id, source);
        writer_id
    }
}

