use crate::{
    compression2::Compression,
    segment::{self, FlushSegment, FullSegment, Segment, WriteSegment},
    persistent_list::{List, ListBuffer},
    id::SeriesId,
    tags::Tags,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct SeriesConfig {
    pub tags: Tags,
    pub compression: Compression,
    pub seg_count: usize,
    pub nvars: usize,
}

#[derive(Clone)]
pub struct SeriesMetadata {
    tags: Tags,
    segment: Segment,
    list: List,
    compression: Compression,
}

impl SeriesMetadata {
    pub fn new(
        conf: SeriesConfig,
        buffer: ListBuffer,
    ) -> Self {

        let SeriesConfig {
            tags,
            compression,
            nvars,
            seg_count,
        } = conf;
        assert_eq!(nvars, compression.len());

        SeriesMetadata {
            segment: segment::Segment::new(seg_count, nvars),
            tags,
            compression,
            list: List::new(buffer),
        }
    }
    pub fn compression(&self) -> &Compression {
        &self.compression
    }

    pub fn segment(&self) -> &Segment {
        &self.segment
    }

    pub fn list(&self) -> &List {
        &self.list
    }
}


