// Copyright (c) 2023 Franco Solleza, Intel Corporation, Brown University
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

use crate::{
    constants::*,
    kafka::{self, KafkaEntry, Producer},
    mem_list::{
        read_only::ReadOnlyDataBlock,
    },
    rdtsc::rdtsc,
    counters,
};
use crossbeam::channel::{unbounded, Receiver, Sender};
use lazy_static::*;
use log::*;
use rand::{thread_rng, Rng};
use std::sync::{Arc, RwLock, atomic::{AtomicUsize, Ordering::SeqCst}};
use std::time::Duration;
use std::thread;
use lzzzz::lz4;

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
    pub static ref PENDING_UNFLUSHED_BYTES: Arc<AtomicUsize> = {
        let x = Arc::new(AtomicUsize::new(0));
        let x2 = x.clone();
        thread::spawn(move || loop {
            let x = x2.load(SeqCst);
            info!("Pending unflushed data bytes: {}", x);
            thread::sleep(Duration::from_secs(5));
        });
        x
    };
    pub static ref PENDING_UNFLUSHED_BLOCKS: Arc<AtomicUsize> = {
        let x = Arc::new(AtomicUsize::new(0));
        let x2 = x.clone();
        thread::spawn(move || loop {
            let x = x2.load(SeqCst);
            info!("Pending unflushed data blocks: {}", x);
            thread::sleep(Duration::from_secs(5));
        });
        x
    };
    pub static ref TOTAL_BYTES_FLUSHED: Arc<AtomicUsize> = {
        let x = Arc::new(AtomicUsize::new(0));
        let x2 = x.clone();
        thread::spawn(move || loop {
            let x = x2.load(SeqCst);
            info!("Total data bytes flushed: {}", x);
            thread::sleep(Duration::from_secs(5));
        });
        x
    };
}

fn flush_worker(chan: Receiver<DataBlock>) {
    debug!("Initing Mach Block Kafka Flusher");
    let mut producer = Producer::new();
    let mut rng = thread_rng();
    let mut partition = rng.gen_range(0i32..PARTITIONS as i32);
    let mut counter = 0;
    while let Ok(block) = chan.recv() {
        let start = rdtsc();
        block.flush(partition, &mut producer);
        TOTAL_BYTES_FLUSHED.fetch_add(block.len, SeqCst);
        PENDING_UNFLUSHED_BYTES.fetch_sub(block.len, SeqCst);
        PENDING_UNFLUSHED_BLOCKS.fetch_sub(1, SeqCst);
        if counter % 1_000 == 0 && counter > 0 {
            partition = rng.gen_range(0i32..PARTITIONS as i32);
        }
        counter += 1;
        counters::DATA_BLOCK_FLUSHER_CYCLES.increment(rdtsc() - start);
    }
    error!("Mach Data Block Kafka Flusher Exited");
}

#[derive(Clone)]
pub struct DataBlock {
    len: usize,
    inner: Arc<RwLock<InnerDataBlock>>,
}

pub fn compress_data_block_bytes(data: &[u8]) -> Vec<u8> {
    let mut v = Vec::new();
    v.extend_from_slice(&data.len().to_be_bytes());
    lz4::compress_to_vec(data, &mut v, BLOCK_COMPRESS_ACC).unwrap();
    debug!("Data block compression result: {} -> {}", data.len(), v.len());
    v
}

pub fn decompress_data_block_bytes(data: &[u8]) -> Vec<u8> {
    let sz = usize::from_be_bytes(data[..8].try_into().unwrap());
    let mut decompressed_block = vec![0u8; sz];
    lz4::decompress(&data[8..], decompressed_block.as_mut_slice()).unwrap();
    decompressed_block
}

impl DataBlock {
    pub fn new(data: &[u8]) -> Self {
        let v = compress_data_block_bytes(data);
        //let v = data;
        let s = Self {
            len: v.len(),
            inner: Arc::new(RwLock::new(InnerDataBlock::Data(v.into()))),
        };
        s.async_flush();
        s
    }

    pub fn async_flush(&self) {
        let x = self.clone();
        PENDING_UNFLUSHED_BLOCKS.fetch_add(1, SeqCst);
        PENDING_UNFLUSHED_BYTES.fetch_add(self.len, SeqCst);
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
                let decompressed_block = decompress_data_block_bytes(&block[..]);
                ReadOnlyDataBlock::from(&decompressed_block[..])
            }
        }
    }
}

enum InnerDataBlock {
    Offset(KafkaEntry),
    Data(Arc<[u8]>),
}
