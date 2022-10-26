use crate::{
    mem_list::{
        data_block::DataBlock,
        metadata_list::{MetadataEntry, MetadataBlock}
    },
    kafka::KafkaEntry,
    constants::*,
};
use serde::*;
use std::convert::From;
use serde_big_array::BigArray;

#[derive(Serialize, Deserialize)]
enum InnerReadOnlyDataBlock {
    Offset(KafkaEntry),
    Data(Box<[u8]>),
}

#[derive(Serialize, Deserialize)]
pub struct ReadOnlyDataBlock {
    inner: InnerReadOnlyDataBlock
}

impl ReadOnlyDataBlock {
    fn new_data(data: &[u8]) -> Self {
        Self {
            inner: InnerReadOnlyDataBlock::Data(data.into()),
        }
    }

    fn new_kafka_entry(entry: &KafkaEntry) -> Self {
        Self {
            inner: InnerReadOnlyDataBlock::Offset(entry.clone())
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
pub struct ReadOnlyMetadataBlock {
    data_blocks: Vec<ReadOnlyDataBlock>,
    kafka: KafkaEntry
}
