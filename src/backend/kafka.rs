use crate::{
    backend::{ByteEntry, FlushEntry},
    flush_buffer::{FlushBuffer, FrozenBuffer},
    tags::{self, Tags},
};
use lazy_static::*;
use rdkafka::{
    config::ClientConfig,
    consumer::base_consumer::BaseConsumer,
    consumer::Consumer,
    error::KafkaError,
    producer::{FutureProducer, FutureRecord},
    topic_partition_list::{Offset, TopicPartitionList},
    util::Timeout,
    Message,
    //message::OwnedHeaders,
};
use serde::*;
use std::{
    convert::TryInto,
    fs::{File, OpenOptions},
    io::{self, prelude::*, SeekFrom},
    mem::size_of,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::SeqCst},
        Arc,
    },
    time::Duration,
};
use uuid::Uuid;

const MAGICSTR: [u8; 12] = *b"kafkabackend";
pub const KAFKA_TAIL_SZ: usize = 0;
pub const KAFKA_HEADER_SZ: usize = size_of::<Header>();
pub const TOPIC: &str = "MACHSTORAGE";
pub const BOOTSTRAP: &str = "localhost:29092";

pub type KafkaBuffer = FlushBuffer<KAFKA_HEADER_SZ, KAFKA_TAIL_SZ>;
pub type KafkaFrozenBuffer<'buffer> = FrozenBuffer<'buffer, KAFKA_HEADER_SZ, KAFKA_TAIL_SZ>;
pub type KafkaEntry<'buffer> = FlushEntry<'buffer, KAFKA_HEADER_SZ, KAFKA_TAIL_SZ>;

fn random_id() -> String {
    Uuid::new_v4().to_hyphenated().to_string()
}

pub fn default_consumer() -> Result<BaseConsumer, Error> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", BOOTSTRAP)
        .set("group.id", random_id())
        .create()?;
    Ok(consumer)
}

#[derive(Debug, Serialize, Deserialize)]
struct Header {
    magic: [u8; 12],
    mint: u64,
    maxt: u64,
    last_offset: i64,
    last_partition: i32,
    last_mint: u64,
    last_maxt: u64,
}

impl Header {
    fn new() -> Self {
        Self {
            magic: MAGICSTR,
            mint: u64::MAX,
            maxt: u64::MAX,
            last_offset: i64::MAX,
            last_partition: i32::MAX,
            last_mint: u64::MAX,
            last_maxt: u64::MAX,
        }
    }
}

#[derive(Debug)]
pub enum Error {
    Kafka(KafkaError),
    KafkaPollTimeout,
    Bincode(bincode::Error),
    ReadVersion,
    MultipleWriters,
    InvalidMagic,
    Tags(crate::tags::Error),
}

impl From<KafkaError> for Error {
    fn from(item: KafkaError) -> Self {
        Error::Kafka(item)
    }
}

impl From<tags::Error> for Error {
    fn from(item: tags::Error) -> Self {
        Error::Tags(item)
    }
}

impl From<bincode::Error> for Error {
    fn from(item: bincode::Error) -> Self {
        Error::Bincode(item)
    }
}

pub struct KafkaWriter {
    producer: FutureProducer,
}

impl KafkaWriter {
    pub fn new() -> Result<Self, Error> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", BOOTSTRAP)
            .create()?;
        Ok(KafkaWriter { producer })
    }

    pub async fn write(&mut self, bytes: &[u8]) -> Result<(i32, i64), Error> {
        let to_send: FutureRecord<str, [u8]> = FutureRecord::to(TOPIC).payload(bytes);
        let dur = Duration::from_secs(0);
        let stat = self.producer.send(to_send, dur).await;
        match stat {
            Ok(x) => Ok(x),
            Err((err, _)) => Err(err.into()),
        }
    }
}

pub struct KafkaListIterator {
    next_offset: i64,
    next_partition: i32,
    next_mint: u64,
    next_maxt: u64,
    consumer: BaseConsumer,
    timeout: Timeout,
    buf: Vec<u8>,
}

impl KafkaListIterator {
    pub fn next_item(&mut self) -> Result<Option<ByteEntry>, Error> {
        self.buf.clear();
        if self.next_offset == i64::MAX {
            return Ok(None);
        }

        let mut tp_list = TopicPartitionList::new();
        tp_list.add_partition_offset(
            TOPIC,
            self.next_partition,
            Offset::Offset(self.next_offset),
        )?;
        self.consumer.assign(&tp_list).unwrap();
        let res = self.consumer.poll(self.timeout.clone());
        let payload: &[u8] = match &res {
            None => return Err(Error::KafkaPollTimeout),
            Some(Err(x)) => return Err(x.clone().into()),
            Some(Ok(msg)) => msg.payload().unwrap(), // we expect there to always be a payload
        };
        self.buf.extend_from_slice(payload);

        // Get header
        let mut off = 0;
        let header: Header = bincode::deserialize(&self.buf[off..off + KAFKA_HEADER_SZ])?;
        if header.magic != MAGICSTR {
            return Err(Error::InvalidMagic);
        }
        off += KAFKA_HEADER_SZ;

        // Get the offsets for the chunk of bytes
        let end = self.buf.len() - KAFKA_TAIL_SZ;

        let f = ByteEntry {
            mint: header.mint,
            maxt: header.maxt,
            bytes: &self.buf[off..end],
        };

        // Update iterator state
        self.next_offset = header.last_offset;
        self.next_partition = header.last_partition;
        self.next_mint = header.last_mint;
        self.next_maxt = header.last_maxt;

        Ok(Some(f))
    }
}

#[derive(Clone)]
pub struct KafkaList {
    inner: Arc<InnerKafkaList>,
    has_writer: Arc<AtomicBool>,
}

impl KafkaList {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(InnerKafkaList::new()),
            has_writer: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn reader(
        &self,
        consumer: BaseConsumer,
        timeout: Timeout,
    ) -> Result<KafkaListIterator, Error> {
        self.inner.read(consumer, timeout)
    }

    pub fn writer(&self) -> Result<KafkaListWriter, Error> {
        if self.has_writer.swap(true, SeqCst) {
            Err(Error::MultipleWriters)
        } else {
            Ok(KafkaListWriter {
                inner: self.inner.clone(),
                has_writer: self.has_writer.clone(),
            })
        }
    }
}

#[derive(Clone)]
pub struct KafkaListWriter {
    inner: Arc<InnerKafkaList>,
    has_writer: Arc<AtomicBool>,
}

impl KafkaListWriter {
    pub fn push(&mut self, writer: &mut KafkaWriter, mut entry: KafkaEntry) -> Result<(), Error> {
        // Safety: Safe because there's only one writer and concurrent readers check versions
        // during each read.
        unsafe {
            Arc::get_mut_unchecked(&mut self.inner).push(
                writer,
                entry.mint,
                entry.maxt,
                &mut entry.buffer,
            )
        }
    }
}

impl Drop for KafkaListWriter {
    fn drop(&mut self) {
        assert!(self.has_writer.swap(false, SeqCst));
    }
}

struct InnerKafkaList {
    header: Header,
    version: Arc<AtomicU64>,
    spin: Arc<AtomicBool>,
}

impl InnerKafkaList {
    fn new() -> Self {
        Self {
            header: Header::new(),
            version: Arc::new(AtomicU64::new(0)),
            spin: Arc::new(AtomicBool::new(false)),
        }
    }

    fn push(
        &mut self,
        producer: &mut KafkaWriter,
        mint: u64,
        maxt: u64,
        buf: &mut KafkaFrozenBuffer,
    ) -> Result<(), Error> {
        self.header.mint = mint;
        self.header.maxt = maxt;

        // Write header - no tail necessary... (I think...)
        bincode::serialize_into(buf.header_mut(), &self.header)?;

        let bytes = buf.bytes();
        let (partition, offset) = async_std::task::block_on(producer.write(bytes))?;

        self.spin.store(true, SeqCst);

        // Then update the metadata
        self.header.last_offset = offset;
        self.header.last_partition = partition;
        self.header.last_mint = mint;
        self.header.last_maxt = maxt;

        // Update versioning for concurrent readers
        self.version.fetch_add(1, SeqCst);

        self.spin.store(false, SeqCst);

        Ok(())
    }

    fn read(&self, consumer: BaseConsumer, timeout: Timeout) -> Result<KafkaListIterator, Error> {
        let v = self.version.load(SeqCst);
        while self.spin.load(SeqCst) {}
        let iterator = KafkaListIterator {
            next_offset: self.header.last_offset,
            next_partition: self.header.last_partition,
            next_mint: self.header.last_mint,
            next_maxt: self.header.last_maxt,
            consumer,
            timeout,
            buf: Vec::new(),
        };
        while self.spin.load(SeqCst) {}
        if v == self.version.load(SeqCst) {
            Ok(iterator)
        } else {
            Err(Error::ReadVersion)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::*;
    use rand::{thread_rng, Rng};

    #[test]
    fn run_test() {
        let mut producer = KafkaWriter::new().unwrap();
        let mut tags = Tags::new();
        tags.insert(("A".to_string(), "B".to_string()));
        tags.insert(("C".to_string(), "D".to_string()));

        let data = (0..3)
            .map(|i| {
                let mut v = vec![i + 1 as u8; 10];
                v
            })
            .collect::<Vec<Vec<u8>>>();

        let mut kafka_list = KafkaList::new();
        let mut writer = kafka_list.writer().unwrap();
        let mut buf = KafkaBuffer::new();
        buf.push_bytes(data[0].as_slice()).unwrap();
        writer
            .push(&mut producer, KafkaEntry::new(0, 5, buf.freeze()))
            .unwrap();
        buf.truncate(0);
        buf.push_bytes(data[1].as_slice()).unwrap();
        writer
            .push(&mut producer, KafkaEntry::new(6, 11, buf.freeze()))
            .unwrap();
        buf.truncate(0);
        buf.push_bytes(data[2].as_slice()).unwrap();
        writer
            .push(&mut producer, KafkaEntry::new(12, 13, buf.freeze()))
            .unwrap();

        let timeout = Timeout::After(Duration::from_secs(1));
        let consumer = default_consumer().unwrap();
        let mut reader = kafka_list.reader(consumer, timeout).unwrap();
        let chunk = reader.next_item().unwrap().unwrap();
        assert_eq!(chunk.bytes, data[2].as_slice());
        assert_eq!(chunk.mint, 12);
        assert_eq!(chunk.maxt, 13);

        let chunk = reader.next_item().unwrap().unwrap();
        assert_eq!(chunk.bytes, data[1].as_slice());
        assert_eq!(chunk.mint, 6);
        assert_eq!(chunk.maxt, 11);

        let chunk = reader.next_item().unwrap().unwrap();
        assert_eq!(chunk.bytes, data[0].as_slice());
        assert_eq!(chunk.mint, 0);
        assert_eq!(chunk.maxt, 5);
    }
}

