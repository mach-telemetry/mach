use crate::{
    compression::Compression,
    segment::{self, FlushSegment, FullSegment, Segment, WriteSegment, ReadSegment},
    persistent_list::{self, List, ListBuffer, ListReader},
    id::SeriesId,
    tags::Tags,
};
use std::sync::Arc;

pub enum Error {
    Segment(segment::Error),
    PersistentList(persistent_list::Error),
}

impl From<persistent_list::Error> for Error {
    fn from(item: persistent_list::Error) -> Self {
        Error::PersistentList(item)
    }
}

impl From<segment::Error> for Error {
    fn from(item: segment::Error) -> Self {
        Error::Segment(item)
    }
}

#[derive(Clone)]
pub struct SeriesConfig {
    pub tags: Tags,
    pub compression: Compression,
    pub seg_count: usize,
    pub nvars: usize,
}

pub struct ReadSeries {
    segment: ReadSegment,
    list: ListReader,
}

#[derive(Clone)]
pub struct Series {
    tags: Tags,
    segment: Segment,
    list: List,
    compression: Compression,
}

impl Series {
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

        Self {
            segment: segment::Segment::new(seg_count, nvars),
            tags,
            compression,
            list: List::new(buffer),
        }
    }

    pub fn snapshot(&self) -> Result<ReadSeries, Error> {
        let segment = self.segment.snapshot()?;
        let list = self.list.read()?;
        Ok(ReadSeries {
            segment,
            list
        })
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


