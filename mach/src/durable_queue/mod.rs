mod kafka_backend;
mod file_backend;

use serde::*;
use enum_dispatch::*;
use std::path::PathBuf;

pub enum Error {
    Kafka(kafka_backend::Error),
    File(file_backend::Error),
}

impl From<kafka_backend::Error> for Error {
    fn from(item: kafka_backend::Error) -> Self {
        Error::Kafka(item)
    }
}

impl From<file_backend::Error> for Error {
    fn from(item: file_backend::Error) -> Self {
        Error::File(item)
    }
}

#[derive(Default, Debug, Clone)]
pub struct Config {
    pub kafka_bootstrap: Option<String>,
    pub directory: Option<PathBuf>,
}

impl Config {
}

pub enum DurableQueue {
    Kafka(kafka_backend::Kafka),
    File(file_backend::FileBackend)
}

impl DurableQueue {
    pub fn writer(&self) -> Result<DurableQueueWriter, Error> {
        match self {
            Self::Kafka(x) => Ok(DurableQueueWriter::Kafka(x.make_writer()?)),
            Self::File(x) => Ok(DurableQueueWriter::File(x.make_writer()?)),
        }
    }

    pub fn reader(&self) -> Result<DurableQueueReader, Error> {
        match self {
            Self::Kafka(x) => Ok(DurableQueueReader::Kafka(x.make_reader()?)),
            Self::File(x) => Ok(DurableQueueReader::File(x.make_reader()?)),
        }
    }
}

pub enum DurableQueueWriter {
    Kafka(kafka_backend::KafkaWriter),
    File(file_backend::FileWriter)
}

impl DurableQueueWriter {
    pub fn write(&mut self, bytes: &[u8]) -> Result<u64, Error> {
        match self {
            Self::Kafka(x) => Ok(x.write(bytes)?),
            Self::File(x) => Ok(x.write(bytes)?),
        }
    }
}

pub enum DurableQueueReader {
    Kafka(kafka_backend::KafkaReader),
    File(file_backend::FileReader)
}

impl DurableQueueReader {
    pub fn read(&mut self, id: u64) -> Result<&[u8], Error> {
        match self {
            Self::Kafka(x) => Ok(x.read(id)?),
            Self::File(x) => Ok(x.read(id)?),
        }
    }
}
