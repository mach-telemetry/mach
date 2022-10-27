use crate::{kafka::KafkaEntry, mem_list::data_block::DataBlock};
use serde::*;
use std::convert::From;

#[derive(Serialize, Deserialize)]
enum InnerReadOnlyDataBlock {
    Offset(KafkaEntry),
    Data(Box<[u8]>),
}

#[derive(Serialize, Deserialize)]
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

#[derive(Serialize, Deserialize)]
pub struct ReadOnlyMetadataEntry {
    block: ReadOnlyDataBlock,
    min: u64,
    max: u64,
}

impl ReadOnlyMetadataEntry {
    pub fn new(block: ReadOnlyDataBlock, min: u64, max: u64) -> Self {
        assert!(min <= max);
        Self { block, min, max }
    }
}

#[derive(Serialize, Deserialize)]
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
}
