use crate::persistent_list::{inner2, Error};
use async_std::channel::{unbounded, Receiver, Sender};
use dashmap::DashMap;
use rand::prelude::*;
use redis::{Commands, Connection, RedisError};
use std::{sync::Arc};


async fn worker(
    mut con: Connection,
    transfer_map: Arc<DashMap<u64, Arc<[u8]>>>,
    queue: Receiver<u64>,
) {
    while let Ok(key) = queue.recv().await {
        let data = transfer_map.get(&key).unwrap().clone();
        let result: Result<(), RedisError> = con.set(key, &data[..]);
        match result {
            Err(err) => {
                panic!("HERE");
            }
            Ok(_) => {}
        }
        transfer_map.remove(&key);
    }
}

pub struct RedisWriter {
    offset: u32,
    prefix: u32,
    transfer_map: Arc<DashMap<u64, Arc<[u8]>>>,
    sender: Sender<u64>,
}

impl RedisWriter {
    pub fn new(con: Connection, transfer_map: Arc<DashMap<u64, Arc<[u8]>>>) -> Self {
        let (sender, receiver) = unbounded();
        let map = transfer_map.clone();
        async_std::task::spawn(worker(con, map, receiver));
        Self {
            offset: 0,
            prefix: thread_rng().gen(),
            transfer_map,
            sender,
        }
    }
}

impl inner2::ChunkWriter for RedisWriter {
    fn write(&mut self, bytes: &[u8]) -> Result<u64, Error> {
        let key = ((self.prefix as u64) << 32) | (self.offset as u64);
        self.offset += 1;
        self.transfer_map.insert(key, bytes.into());
        self.sender.try_send(key).unwrap();
        Ok(key)
    }
}

pub struct RedisReader {
    con: Connection,
    transfer_map: Arc<DashMap<u64, Arc<[u8]>>>,
    local: Vec<u8>,
}

impl RedisReader {
    pub fn new(con: Connection, transfer_map: Arc<DashMap<u64, Arc<[u8]>>>) -> Self {
        Self {
            con,
            transfer_map,
            local: Vec::new(),
        }
    }
}

impl inner2::ChunkReader for RedisReader {
    fn read(&mut self, offset: u64) -> Result<&[u8], Error> {
        self.local.clear();
        match self.transfer_map.get(&offset) {
            Some(x) => {
                let x = x.clone();
                self.local.extend_from_slice(&x[..]);
            },
            None => {
                match self.con.get::<u64, Vec<u8>>(offset) {
                    Ok(x) => {
                        self.local = x;
                    },
                    Err(x) => panic!(),
                }
            }
        }
        Ok(self.local.as_slice())
    }
}

