use crate::{kafka::KafkaEntry, mem_list::data_block::DataBlock};
use serde::*;
use std::convert::From;
use log::*;

#[derive(Clone, Serialize, Deserialize)]
enum InnerReadOnlyDataBlock {
    Offset(KafkaEntry),
    Data(Box<[u8]>),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ReadOnlyDataBlock {
    inner: InnerReadOnlyDataBlock,
}

impl ReadOnlyDataBlock {
    fn new_data(data: &[u8]) -> Self {
        Self {
            inner: InnerReadOnlyDataBlock::Data(data.into()),
        }
    }

    fn new_kafka_entry(entry: &KafkaEntry) -> Self {
        Self {
            inner: InnerReadOnlyDataBlock::Offset(entry.clone()),
        }
    }

    pub fn load(&mut self) {
        match &self.inner {
            InnerReadOnlyDataBlock::Data(_) => {}
            InnerReadOnlyDataBlock::Offset(x) => {
                let mut v = Vec::new();
                x.load(&mut v).unwrap();
                self.inner = InnerReadOnlyDataBlock::Data(v.into_boxed_slice());
            }
        }
    }

    pub fn bytes(&self) -> &[u8] {
        match &self.inner {
            InnerReadOnlyDataBlock::Data(x) => &x[..],
            _ => panic!("load first"),
        }
    }
}

impl From<&[u8]> for ReadOnlyDataBlock {
    fn from(x: &[u8]) -> Self {
        Self::new_data(x)
    }
}

impl From<&KafkaEntry> for ReadOnlyDataBlock {
    fn from(x: &KafkaEntry) -> Self {
        Self::new_kafka_entry(x)
    }
}

impl From<&DataBlock> for ReadOnlyDataBlock {
    fn from(x: &DataBlock) -> Self {
        x.read_only()
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ReadOnlyMetadataEntry {
    pub block: ReadOnlyDataBlock,
    pub min: u64,
    pub max: u64,
}

impl ReadOnlyMetadataEntry {
    pub fn new(block: ReadOnlyDataBlock, min: u64, max: u64) -> Self {
        assert!(min <= max);
        Self { block, min, max }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ReadOnlyMetadataBlock {
    data_blocks: Vec<ReadOnlyMetadataEntry>,
    kafka_min: u64,
    kafka_max: u64,
    kafka: KafkaEntry,
}

impl ReadOnlyMetadataBlock {
    pub fn new(
        data_blocks: Vec<ReadOnlyMetadataEntry>,
        kafka: KafkaEntry,
        kafka_min: u64,
        kafka_max: u64,
    ) -> Self {
        Self {
            data_blocks,
            kafka_min,
            kafka_max,
            kafka,
        }
    }

    pub fn load_next(&self) -> Option<Self> {
        if self.kafka.is_empty() {
            None
        } else {
            let mut v = Vec::new();
            info!("Reading kafka entry: {:?}", self.kafka);
            self.kafka.load(&mut v).unwrap();
            Some(bincode::deserialize(v.as_slice()).unwrap())
        }
    }

    pub fn len(&self) -> usize {
        self.data_blocks.len()
    }

    pub fn get_data_bytes(&mut self, idx: usize) -> &[u8] {
        let block = &mut self.data_blocks[idx].block;
        block.load();
        block.bytes()
    }
}
