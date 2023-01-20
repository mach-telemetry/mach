use crate::{
    active_block::{ActiveBlock, ActiveBlockWriter},
    active_segment::{ActiveSegment, ActiveSegmentWriter, PushStatus},
    mem_list::metadata_list::MetadataList,
    sample::SampleType,
    source::{Source, SourceConfig, SourceId},
    segment::SegmentRef,
    counters::WRITER_CYCLES,
    rdtsc::rdtsc,
};
use dashmap::DashMap;
use log::*;
use serde::*;
use std::convert::From;
use std::ops::Deref;
use std::sync::{Arc, atomic::{AtomicBool, Ordering::SeqCst}};
use crossbeam::channel::{Sender, Receiver, unbounded};

#[derive(Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct SourceRef(pub u64);

impl Deref for SourceRef {
    type Target = u64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<u64> for SourceRef {
    fn from(id: u64) -> Self {
        SourceRef(id)
    }
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct WriterId(pub u64);
impl Deref for WriterId {
    type Target = u64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<u64> for WriterId {
    fn from(id: u64) -> Self {
        WriterId(id)
    }
}

pub struct Writer {
    source_table: Arc<DashMap<SourceId, Source>>, // Global Source Table
    segments: Vec<ActiveSegmentWriter>,
    source_configs: Vec<SourceConfig>,
    active_block: ActiveBlock,
    //active_block_writer: ActiveBlockWriter,
    active_block_channel: Sender<WorkerMsg>,
}

impl Writer {
    pub fn new(source_table: Arc<DashMap<SourceId, Source>>) -> Self {
        let (active_block, active_block_writer) = ActiveBlock::new();
        let (tx, rx) = unbounded();
        std::thread::spawn(move || {
            active_block_worker(active_block_writer, rx);
        });
        Self {
            source_table,
            segments: Vec::new(),
            source_configs: Vec::new(),
            active_block,
            //active_block_writer,
            active_block_channel: tx,
        }
    }

    pub fn push(&mut self, id: SourceRef, ts: u64, sample: &[SampleType]) -> Result<(), &str> {
        let idx = (*id) as usize;
        let seg = &mut self.segments[idx];
        match seg.push(ts, sample) {
            PushStatus::Full => {
                debug!("Active segment is full");
                self.active_block_channel.send(WorkerMsg::Flush(id));
                Ok(())
            },
            PushStatus::ErrorFull => {
                Err("Active segment is full")
            },
            PushStatus::Ok => {
                Ok(())
            }

        }
    }

    pub fn add_source(&mut self, config: SourceConfig) -> SourceRef {
        let source_id = config.id;
        let (metadata_list, metadata_list_writer) = MetadataList::new();
        let (active_segment, active_segment_writer) = ActiveSegment::new(config.types.as_slice());
        self.active_block_channel.send(WorkerMsg::Source((active_segment.clone(), config.clone())));
        self.active_block
            .add_source(source_id, metadata_list_writer);
        let source = Source {
            config: config.clone(),
            active_segment,
            metadata_list,
            active_block: self.active_block.clone(),
        };
        self.source_table.insert(source_id, source);
        self.segments.push(active_segment_writer);
        self.source_configs.push(config);
        SourceRef((self.segments.len() - 1) as u64)
    }
}

enum WorkerMsg {
    Flush(SourceRef),
    Source((ActiveSegment, SourceConfig)),
}

fn active_block_worker(active_block_writer: ActiveBlockWriter, rx: Receiver<WorkerMsg>) {
    let mut worker = ActiveBlockWorker::new(active_block_writer);
    while let Ok(msg) = rx.recv() {
        match msg {
            WorkerMsg::Flush(id) => {
                let start = rdtsc();
                worker.flush(id);
                WRITER_CYCLES.increment(rdtsc() - start);
            },
            WorkerMsg::Source((w, c)) => worker.add_source(w, c),
        }
    }
}

struct ActiveBlockWorker {
    active_block_writer: ActiveBlockWriter,
    segments: Vec<ActiveSegment>,
    source_configs: Vec<SourceConfig>,
}

impl ActiveBlockWorker {
    fn new(w: ActiveBlockWriter) -> Self {
        ActiveBlockWorker {
            active_block_writer: w,
            segments: Vec::new(),
            source_configs: Vec::new(),
        }
    }

    fn flush(&mut self, id: SourceRef) {
        let idx = id.0 as usize;
        let conf = &self.source_configs[idx];
        let seg = &mut self.segments[idx];
        let segment_ref = seg.as_segment_ref();
        self.active_block_writer
            .push(conf.id, segment_ref, &conf.compression);
        unsafe { seg.reset() };
    }

    fn add_source(&mut self, w: ActiveSegment, c: SourceConfig) {
        self.segments.push(w);
        self.source_configs.push(c);
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use crate::compression::{Compression, CompressionScheme};
    use crate::field_type::FieldType;
    use crate::segment::SegmentIterator;
    use crate::test_utils::*;
    use crate::utils::now_in_micros;
    use env_logger;
    use log::info;
    use rand::{thread_rng, Rng};
    use std::collections::HashSet;

    #[test]
    fn test() {
        env_logger::init();
        let n_samples: usize = 120_000;
        let n_sources = 1;
        let source_table = Arc::new(DashMap::new());
        let field_type: &[FieldType] = &[FieldType::Bytes, FieldType::F64];
        let mut writer = Writer::new(source_table.clone());
        for i in 0..n_sources {
            let source_config = SourceConfig {
                id: SourceId(i),
                types: field_type.into(),
                compression: Compression::new(vec![
                    CompressionScheme::delta_of_delta(),
                    CompressionScheme::lz4(),
                ]),
            };
            let source_ref = writer.add_source(source_config);
        }
        let samples: Vec<Vec<SampleType>> = {
            let samples = random_samples(field_type, 1_000, 1024..1024 * 8);
            let mut samples_transposed = Vec::new();
            for i in 0..1_000 {
                let mut s = Vec::new();
                s.push(samples[0][i].clone());
                s.push(samples[1][i].clone());
                samples_transposed.push(s);
            }
            samples_transposed
        };

        let mut idx = vec![0; n_sources as usize];
        let mut set = HashSet::new();

        let mut rng = thread_rng();
        let mut counter = 0;

        let mut expected_timestamps = Vec::new();
        let mut expected_samples: Vec<Vec<SampleType>> = Vec::new();
        while set.len() < n_sources as usize {
            let i = rng.gen_range(0..n_sources) as usize;
            if idx[i] < n_samples {
                let sample_idx = idx[i];
                let ts = now_in_micros();
                writer.push(SourceRef(i as u64), ts, &samples[sample_idx % 1_000]);
                if i == 0 {
                    expected_timestamps.push(ts);
                    expected_samples.push(samples[sample_idx % 1_000].clone());
                }
                idx[i] += 1;
            } else {
                set.insert(i);
            }
        }
        expected_timestamps.reverse();
        expected_samples.reverse();

        info!("Querying");

        //assert_eq!(idx, vec![n_samples; n_sources as usize]);
        let source = source_table.get(&SourceId(0)).unwrap().clone();
        let mut snap = source.snapshot().into_snapshot_iterator();
        info!("Snapshot made, iterating over snapshot");
        let mut timestamps = Vec::new();
        let mut samples: Vec<Vec<SampleType>> = Vec::new();
        let mut counter = 0;
        while let Some(seg) = snap.next_segment() {
            let mut iter = SegmentIterator::new(&seg);
            while let Some((ts, sample)) = iter.next_sample() {
                timestamps.push(ts);
                samples.push(sample.into());
            }
            counter += 1;
        }
        info!("Done iterating over snapshot");
        assert_eq!(timestamps.len(), expected_timestamps.len());
        assert_eq!(timestamps, expected_timestamps);
        assert_eq!(samples, expected_samples);
    }
}
