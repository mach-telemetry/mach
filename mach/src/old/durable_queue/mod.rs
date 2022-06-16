mod file_backend;
mod kafka_backend;

use file_backend::FileReader;
use kafka_backend::KafkaReader;
use serde::*;
use std::path::PathBuf;

use std::sync::atomic::AtomicUsize;

lazy_static::lazy_static! {
    pub static ref TOTAL_SZ: AtomicUsize = AtomicUsize::new(0);
}

#[derive(Debug)]
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

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct NoopConfig {}
impl NoopConfig {
    fn make(&self) -> Result<InnerDurableQueue, Error> {
        Ok(InnerDurableQueue::Noop)
    }
    pub fn config(self) -> QueueConfig {
        QueueConfig::Noop
    }
}


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct KafkaConfig {
    pub bootstrap: String,
    pub topic: String,
}

impl KafkaConfig {
    fn make(&self) -> Result<InnerDurableQueue, Error> {
        Ok(InnerDurableQueue::Kafka(kafka_backend::Kafka::new(
            &self.bootstrap,
            &self.topic,
        )?))
    }
    pub fn config(self) -> QueueConfig {
        QueueConfig::Kafka(self)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FileConfig {
    pub dir: PathBuf,
    pub file: String,
}

impl FileConfig {
    fn make(&self) -> Result<InnerDurableQueue, Error> {
        Ok(InnerDurableQueue::File(file_backend::FileBackend::new(
            &self.dir, &self.file,
        )?))
    }
    pub fn config(self) -> QueueConfig {
        QueueConfig::File(self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueueConfig {
    Kafka(KafkaConfig),
    File(FileConfig),
    Noop,
}

impl QueueConfig {
    pub fn make(self) -> Result<DurableQueue, Error> {
        let config = self.clone();
        let d = match self {
            Self::Kafka(x) => DurableQueue {
                config,
                inner: x.make()?,
            },
            Self::File(x) => DurableQueue {
                config,
                inner: x.make()?,
            },
            Self::Noop => DurableQueue {
                config,
                inner: InnerDurableQueue::Noop,
            },
        };
        Ok(d)
    }
}

pub struct DurableQueue {
    config: QueueConfig,
    inner: InnerDurableQueue,
}

impl DurableQueue {
    pub fn config(&self) -> QueueConfig {
        self.config.clone()
    }
    pub fn writer(&self) -> Result<DurableQueueWriter, Error> {
        match &self.inner {
            InnerDurableQueue::Kafka(x) => Ok(DurableQueueWriter::Kafka(x.make_writer()?)),
            InnerDurableQueue::File(x) => Ok(DurableQueueWriter::File(x.make_writer()?)),
            InnerDurableQueue::Noop => Ok(DurableQueueWriter::Noop),
        }
    }

    pub fn reader(&self) -> Result<DurableQueueReader, Error> {
        match &self.inner {
            InnerDurableQueue::Kafka(x) => Ok(DurableQueueReader::Kafka(x.make_reader()?)),
            InnerDurableQueue::File(x) => Ok(DurableQueueReader::File(x.make_reader()?)),
            InnerDurableQueue::Noop => unimplemented!(),
        }
    }
}

enum InnerDurableQueue {
    Kafka(kafka_backend::Kafka),
    File(file_backend::FileBackend),
    Noop,
}

pub enum DurableQueueWriter {
    Kafka(kafka_backend::KafkaWriter),
    File(file_backend::FileWriter),
    Noop,
}

impl DurableQueueWriter {
    pub fn write(&mut self, bytes: &[u8]) -> Result<u64, Error> {
        match self {
            Self::Kafka(x) => Ok(x.write(bytes)?),
            Self::File(x) => Ok(x.write(bytes)?),
            Self::Noop => Ok(0),
        }
    }
}

pub enum DurableQueueReader {
    Kafka(kafka_backend::KafkaReader),
    File(file_backend::FileReader),
}

impl DurableQueueReader {
    pub fn from_config(config: QueueConfig) -> Result<Self, Error> {
        match config {
            QueueConfig::Kafka(cfg) => Ok(DurableQueueReader::Kafka(KafkaReader::new(
                cfg.bootstrap.clone(),
                cfg.topic,
            )?)),
            QueueConfig::File(cfg) => {
                let file = cfg.dir.join(cfg.file);
                Ok(DurableQueueReader::File(FileReader::new(&file)?))
            }
            QueueConfig::Noop => unimplemented!(),
        }
    }

    pub fn read(&mut self, id: u64) -> Result<&[u8], Error> {
        match self {
            Self::Kafka(x) => Ok(x.read(id)?),
            Self::File(x) => Ok(x.read(id)?),
        }
    }
}
