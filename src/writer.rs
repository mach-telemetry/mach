use dashmap::DashMap;
use std::{
    collections::HashMap,
};
use crate::{
    active_segment::ActiveSegmentWriter,
    types::Sample,
    tsdb2::{RefId, SeriesId, MemSeriesWriter, SeriesMetadata},
};

