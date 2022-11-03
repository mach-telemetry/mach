use crate::{
    constants::*,
    kafka::{self, KafkaEntry, Producer},
    mem_list::read_only::ReadOnlyDataBlock,
};
use crossbeam::channel::{unbounded, Receiver, Sender};
use lazy_static::*;
use log::*;
use rand::{thread_rng, Rng};
use std::sync::{Arc, RwLock};

lazy_static! {
    static ref DATA_BLOCK_WRITER: Sender<DataBlock> = {
        kafka::init();
        let (tx, rx) = unbounded();
        for idx in 0..N_FLUSHERS {
            let rx = rx.clone();
            std::thread::Builder::new()
                .name(format!("Mach: Data Block Kafka Flusher {}", idx))
                .spawn(move || {
                    flush_worker(rx);
                })
                .unwrap();
        }
        tx
    };
}

fn flush_worker(chan: Receiver<DataBlock>) {
    info!("Initing Mach Block Kafka Flusher");
    let mut producer = Producer::new();
    let mut rng = thread_rng();
    let mut partition = rng.gen_range(0i32..PARTITIONS as i32);
    let mut counter = 0;
    while let Ok(block) = chan.recv() {
        block.flush(partition, &mut producer);
        if counter % 1_000 == 0 && counter > 0 {
            partition = rng.gen_range(0i32..PARTITIONS as i32);
        }
        counter += 1;
    }
    error!("Mach Data Block Kafka Flusher Exited");
}

#[derive(Clone)]
pub struct DataBlock {
    inner: Arc<RwLock<InnerDataBlock>>,
}

impl DataBlock {
    pub fn new(data: &[u8]) -> Self {
        let s = Self {
            inner: Arc::new(RwLock::new(InnerDataBlock::Data(data.into()))),
        };
        s.async_flush();
        s
    }

    pub fn async_flush(&self) {
        let x = self.clone();
        DATA_BLOCK_WRITER.try_send(x).unwrap();
    }

    pub fn flush(&self, partition: i32, producer: &mut Producer) {
        let read_guard = self.inner.read().unwrap();
        match &*read_guard {
            InnerDataBlock::Offset(_) => {}
            InnerDataBlock::Data(block) => {
                let block = block.clone();
                drop(read_guard);
                let kafka_entry = producer.send(partition, &block[..]);
                *self.inner.write().unwrap() = InnerDataBlock::Offset(kafka_entry);
            }
        }
    }

    pub fn read_only(&self) -> ReadOnlyDataBlock {
        let read_guard = self.inner.read().unwrap();
        match &*read_guard {
            InnerDataBlock::Offset(x) => ReadOnlyDataBlock::from(x),
            InnerDataBlock::Data(block) => {
                let block = block.clone();
                drop(read_guard);
                ReadOnlyDataBlock::from(&block[..])
            }
        }
    }
}

enum InnerDataBlock {
    Offset(KafkaEntry),
    Data(Arc<[u8]>),
}
