use crate::{
    active_block::{ActiveBlock, ActiveBlockWriter},
    active_segment::{ActiveSegment, ActiveSegmentWriter},
    mem_list::metadata_list::MetadataList,
    sample::SampleType,
    source::{Source, SourceConfig, SourceId},
};
use dashmap::DashMap;
use serde::*;
use std::convert::From;
use std::ops::Deref;
use std::sync::Arc;

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
    active_block_writer: ActiveBlockWriter,
}

impl Writer {
    pub fn new(source_table: Arc<DashMap<SourceId, Source>>) -> Self {
        let (active_block, active_block_writer) = ActiveBlock::new();
        Self {
            source_table,
            segments: Vec::new(),
            source_configs: Vec::new(),
            active_block,
            active_block_writer,
        }
    }

    pub fn push(&mut self, id: SourceRef, ts: u64, sample: &[SampleType]) {
        let idx = (*id) as usize;
        let seg = &mut self.segments[idx];
        if seg.push(ts, sample).is_full() {
            let segment_ref = seg.as_segment_ref();
            let conf = &self.source_configs[idx];
            self.active_block_writer
                .push(conf.id, segment_ref, &conf.compression);
        }
    }

    pub fn add_source(&mut self, config: SourceConfig) -> SourceRef {
        let source_id = config.id;
        let (metadata_list, metadata_list_writer) = MetadataList::new();
        let (active_segment, active_segment_writer) = ActiveSegment::new(config.types.as_slice());
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::compression::{Compression, CompressionScheme};
    use crate::field_type::FieldType;
    use crate::test_utils::*;
    use rand::{thread_rng, Rng};
    use std::collections::HashSet;
    use crate::utils::now_in_micros;
    use env_logger;

    #[test]
    fn test() {
        env_logger::init();
        let n_samples = 1_000_000;
        let n_sources = 100;
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
            let samples = random_samples(field_type, n_samples);
            let mut samples_transposed = Vec::new();
            for i in 0..n_samples {
                let mut s = Vec::new();
                s.push(samples[0][i].clone());
                s.push(samples[1][i].clone());
                samples_transposed.push(s);
            }
            samples_transposed
        };

        assert_eq!(samples.len(), n_samples);
        let mut idx = vec![0; n_sources as usize];
        let mut set = HashSet::new();

        let mut rng = thread_rng();
        while set.len() < n_sources as usize {
            let i = rng.gen_range(0..n_sources) as usize;
            if idx[i] < n_samples {
                let sample_idx = idx[i];
                writer.push(SourceRef(i as u64), now_in_micros(), &samples[sample_idx]);
                idx[i] += 1;
            } else {
                set.insert(i);
            }
        }

        assert_eq!(idx, vec![n_samples; n_sources as usize]);
        //let source = source_table.get(&SourceId(0)).unwrap().clone();
    }
}
