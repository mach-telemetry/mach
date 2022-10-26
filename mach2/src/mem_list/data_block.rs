use std::sync::{Arc, RwLock};
use crate::{
    kafka::{Producer, KafkaEntry},
    constants::*,
};
use lazy_static::*;
use crossbeam::channel::{Sender, Receiver, unbounded};
use rand::{thread_rng, Rng};
use serde::{Serialize, Deserialize};

lazy_static! {
    static ref DATA_BLOCK_WRITER: Sender<DataBlock> = {
        let (tx, rx) = unbounded();
        for idx in 0..N_FLUSHERS {
            let rx = rx.clone();
            std::thread::Builder::new().name(format!("Mach: Data Block Kafka Flusher {}", idx)).spawn(move || {
                flush_worker(rx);
            }).unwrap();
        }
        tx
    };
}

fn flush_worker(chan: Receiver<DataBlock>) {
    let mut producer = Producer::new();
    let mut rng = thread_rng();
    let mut partition = rng.gen_range(0i32..PARTITIONS as i32);
    let mut counter = 0;
    while let Ok(block) = chan.recv() {
        block.flush(partition, &mut producer);
        if counter % 1_000 == 0 && counter > 0 {
            partition = rng.gen_range(0i32..PARTITIONS as i32);
        }
    }
}

#[derive(Clone)]
pub struct DataBlock {
    inner: Arc<RwLock<InnerDataBlock>>
}

impl DataBlock {
    pub fn async_flush(&self) {
        let x = self.clone();
        DATA_BLOCK_WRITER.try_send(x).unwrap();
    }

    pub fn flush(&self, partition: i32, producer: &mut Producer) {
        let read_guard = self.inner.read().unwrap();
        match &*read_guard {
            InnerDataBlock::Offset(_) => {},
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
        let inner = match &*read_guard {
            InnerDataBlock::Offset(x) => InnerReadOnlyDataBlock::Offset(x.clone()),
            InnerDataBlock::Data(block) => {
                let block = block.clone();
                drop(read_guard);
                InnerReadOnlyDataBlock::Data(block[..].into())
            }
        };
        ReadOnlyDataBlock {
            inner
        }
    }
}

enum InnerDataBlock {
    Offset(KafkaEntry),
    Data(Arc<[u8]>),
}

#[derive(Serialize, Deserialize)]
enum InnerReadOnlyDataBlock {
    Offset(KafkaEntry),
    Data(Box<[u8]>),
}

#[derive(Serialize, Deserialize)]
pub struct ReadOnlyDataBlock {
    inner: InnerReadOnlyDataBlock
}

