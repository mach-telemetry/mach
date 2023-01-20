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

use crate::{constants::*, kafka::KafkaEntry};
use kafka::{
    client::RequiredAcks,
    producer::{Producer as OgProducer, Record},
};
use std::{
    ops::{Deref, DerefMut},
    time::Duration,
};

pub struct Producer(OgProducer);

impl Deref for Producer {
    type Target = OgProducer;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Producer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Producer {
    pub fn new() -> Self {
        let bootstraps = BOOTSTRAPS.split(',').map(String::from).collect();
        let client = OgProducer::from_hosts(bootstraps)
            .with_ack_timeout(Duration::from_millis(10_000))
            .with_required_acks(RequiredAcks::All)
            .create()
            .unwrap();
        Self(client)
    }

    pub fn send(&mut self, partition: i32, item: &[u8]) -> KafkaEntry {
        let mut start = 0;
        //let t = Instant::now();

        let producer: &mut OgProducer = &mut self.0;

        let mut items = Vec::with_capacity(5);

        while start < item.len() {
            let end = item.len().min(start + MAX_MSG_SZ);

            loop {
                let mut erred = false;

                let reqs =
                    &[Record::from_value(TOPIC, &item[start..end]).with_partition(partition)];

                let result = producer.send_all(reqs).unwrap();

                for topic in result.iter() {
                    for partition in topic.partition_confirms.iter() {
                        let p = partition.partition;
                        let o = match partition.offset {
                            Ok(o) => o,
                            Err(_) => {
                                erred = true;
                                std::thread::sleep(Duration::from_millis(500));
                                eprintln!("Retrying");
                                break;
                            }
                        };
                        items.push((p, o))
                    }
                }

                if !erred {
                    start = end;
                    break;
                }
            }
        }
        //let elapsed = t.elapsed();
        //let flush_rate = <f64 as num::NumCast>::from(item.len()).unwrap() / elapsed.as_secs_f64();
        //println!(
        //    "Flush: {} bytes, {} secs, {} rate, iters {}, retries: {}",
        //    item.len(),
        //    t.elapsed().as_secs_f64(),
        //    flush_rate,
        //    num_iters,
        //    num_retries
        //);

        //TOTAL_MB_WRITTEN.fetch_add(item.len(), SeqCst);
        KafkaEntry::from(items)
    }
}
