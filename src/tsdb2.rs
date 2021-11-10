use std::{
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering::SeqCst},
    },
    collections::{BinaryHeap, HashMap},
    cmp::Reverse,
};
use dashmap::DashMap;
use serde::*;
use crate::{
    active_segment::{ActiveSegment, ActiveSegmentWriter},
    types::{Sample, Schema, Type},
    ids::{SeriesId, RefId},
};

pub enum Error {
    InvalidRefId,
}

struct MemSeries {
    active_segments: Vec<ActiveSegment>,
}

impl MemSeries {
    fn writer(&self) -> MemSeriesWriter {
        MemSeriesWriter {
            active_segments: self.active_segments.iter().map(|x| x.writer()).collect(),
            current: 0,
            flushed: 0,
        }
    }
}

struct MemSeriesWriter {
    active_segments: Vec<ActiveSegmentWriter>,
    current: usize,
    flushed: usize,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SeriesOptions {
    schema: Vec<Type>,
}

impl SeriesOptions {
    fn set_schema(mut self, schema: Vec<Type>) -> Self {
        self.schema = schema;
        self
    }
}

impl Default for SeriesOptions {
    fn default() -> Self {
        Self {
            schema: Vec::new()
        }
    }
}

pub struct SeriesMetadata {
    options: SeriesOptions,
    mem_series: MemSeries,
    thread_id: AtomicUsize,
}

pub struct PushResult {
}

pub struct Writer {
    pub series_map: Arc<DashMap<SeriesId, SeriesMetadata>>,
    ref_map: HashMap<SeriesId, RefId>,

    // Reference these items in the push method
    // slots may be empty
    mem_series: Vec<Option<MemSeriesWriter>>,
    active_segment: Vec<Option<ActiveSegmentWriter>>, // for cache friendlyness, set this aside

    // Free mem series vec indices to write to, in increasing order.
    free_ref_ids: BinaryHeap<Reverse<usize>>,

    // Shared buffer for the compressor and the block writer
    //buf: [u8; BLOCKSZ],
    //block_writer: ThreadFileWriter,
    //thread_id: usize,
}

impl Writer {

    pub fn lookup(&mut self, id: SeriesId) -> Option<RefId> {
        if let Some(idx) = self.ref_map.get(&id) {
            Some(*idx)
        } else if let Some(ser) = self.series_map.get_mut(&id) {
            let writer = ser.mem_series.writer();
            // TODO: Implement moving here
            let refid = RefId(self.mem_series.len() as u64);
            self.ref_map.insert(id, refid);
            Some(refid)
        } else {
            None
        }
    }

    pub fn push(
        &mut self,
        reference_id: RefId,
        series_id: SeriesId,
        ts: u64,
        sample: Sample,
    ) -> Result<PushResult, Error> {

        let id = *reference_id as usize;
        let active_segment = match self.active_segment[id].as_mut() {
            Some(x) => x,
            None => return Err(Error::InvalidRefId),
        };

        let segment_len = unsafe { active_segment.push(ts, &sample.data)};

        Ok(PushResult { })
    }

}



