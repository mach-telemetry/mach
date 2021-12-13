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

const MAGICSTR: [u8; 12] = *b"kafkabackend";
pub const KAFKA_TAIL_SZ: usize = 0;
pub const KAFKA_HEADER_SZ: usize = size_of::<Header>();
pub const TOPIC: &str = "MACHSTORAGE";
pub const BOOTSTRAP: &str = "localhost:29092";

pub type KafkaBuffer = FlushBuffer<KAFKA_HEADER_SZ, KAFKA_TAIL_SZ>;
pub type KafkaFrozenBuffer<'buffer> = FrozenBuffer<'buffer, KAFKA_HEADER_SZ, KAFKA_TAIL_SZ>;
pub type KafkaEntry<'buffer> = FlushEntry<'buffer, KAFKA_HEADER_SZ, KAFKA_TAIL_SZ>;

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

pub struct KafkaIterator {
    next_offset: i64,
    next_partition: i32,
    next_mint: u64,
    next_maxt: u64,
    consumer: BaseConsumer,
    timeout: Timeout,
    buf: Vec<u8>,
}

impl KafkaIterator {
    pub fn next_item(&mut self) -> Result<Option<ByteEntry>, Error> {
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
        let payload = match &res {
            None => return Err(Error::KafkaPollTimeout),
            Some(Err(x)) => return Err(x.clone().into()),
            Some(Ok(msg)) => msg.payload().unwrap(), // we expect there to always be a payload
        };

        Ok(None)

        //res.payload()
        //println!("{}", std::str::from_utf8(res.payload().unwrap()).unwrap());

        //let mut file = self.file.as_mut().unwrap();
        //let meta = file.metadata().unwrap();
        //file.seek(SeekFrom::Start(self.next_offset))?;
        //let next_bytes = self.next_bytes as usize;
        //self.buf.resize(next_bytes, 0u8);
        //let bytes = file.read(&mut self.buf[..next_bytes])?;
        //assert_eq!(bytes, next_bytes);

        //let mut off = 0;

        //// Get header
        //let header: Header = bincode::deserialize(&self.buf[off..off + HEADERSZ])?;
        //if header.magic != MAGICSTR {
        //    return Err(Error::InvalidMagic);
        //}
        //off += HEADERSZ;

        //// Get the offsets for the chunk of bytes
        //let end = self.buf.len() - TAILSZ;

        //let f = ByteEntry {
        //    mint: header.mint,
        //    maxt: header.maxt,
        //    bytes: &self.buf[off..end],
        //};

        //// Update iterator state
        //self.next_offset = header.last_offset;
        //self.next_mint = header.last_mint;
        //self.next_maxt = header.last_maxt;
        //self.next_bytes = header.last_bytes;
        //if self.next_file_id != header.last_file_id {
        //    self.file = None;
        //    self.next_file_id = header.last_file_id;
        //}
        //Ok(Some(f))
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

    //fn read(&self) -> Result<KafkaIterator, Error> {
    //    let v = self.version.load(SeqCst);
    //    while self.spin.load(SeqCst) {}
    //    let iterator = KafkaIterator {
    //        next_offset: self.header.last_offset,
    //        next_partition: self.header.last_partition,
    //        next_mint: self.header.last_mint,
    //        next_maxt: self.header.last_maxt,
    //        buf: Vec::new(),
    //    };
    //    while self.spin.load(SeqCst) {}
    //    if v == self.version.load(SeqCst) {
    //        Ok(iterator)
    //    } else {
    //        Err(Error::ReadVersion)
    //    }
    //}
}
