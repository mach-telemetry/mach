#![allow(dead_code)]
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


pub use kafka::client::{KafkaClient, ProduceMessage, RequiredAcks};
use kafka::producer::{Producer as OgProducer, Record};
use lazy_static::*;
use num::NumCast;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
};
use std::ops::{Deref, DerefMut};
use std::sync::{
    atomic::{AtomicUsize, Ordering::SeqCst},
    Arc,
};
use std::thread;
use std::time::Duration;

lazy_static! {
    static ref TOTAL_MB_WRITTEN: Arc<AtomicUsize> = {
        let x = Arc::new(AtomicUsize::new(0));
        let x2 = x.clone();
        thread::spawn(move || {
            let mut last = 0.;
            loop {
                let x = x2.load(SeqCst);
                let x2 = <f64 as NumCast>::from(x).unwrap();
                let mb = (x2 - last) / 1_000_000.;
                println!("Total mb written to Kafka: {} mbps, {} total mb", mb, x);
                last = x2;
                thread::sleep(Duration::from_secs(1));
            }
        });
        x
    };
}

#[derive(Copy, Clone)]
pub struct KafkaTopicOptions {
    pub num_partitions: i32,
    pub num_replicas: i32,
}

impl Default for KafkaTopicOptions {
    fn default() -> Self {
        Self {
            num_replicas: 3,
            num_partitions: 3,
        }
    }
}

pub fn make_topic(bootstrap: &str, topic: &str, opts: KafkaTopicOptions) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let client: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .unwrap();
    let admin_opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(3)));
    let min_insync_replicas = opts.num_replicas.to_string();
    let topics = &[NewTopic {
        name: topic,
        num_partitions: opts.num_partitions,
        replication: TopicReplication::Fixed(opts.num_replicas),
        config: vec![("min.insync.replicas", min_insync_replicas.as_str())],
    }];
    rt.block_on(client.create_topics(topics, &admin_opts))
        .unwrap();
    println!("MADE TOPIC {}", topic);
}

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
    pub fn new(bootstraps: &str, acks_all: bool) -> Self {
        let bootstraps = bootstraps.clone().split(',').map(String::from).collect();
        let acks = match acks_all {
            true => RequiredAcks::All,
            false => RequiredAcks::One,
        };
        let client = OgProducer::from_hosts(bootstraps)
            .with_ack_timeout(Duration::from_millis(10_000))
            .with_required_acks(acks)
            .create()
            .unwrap();
        Self(client)
    }

    pub fn send(&mut self, topic: &str, partition: i32, item: &[u8]) {
        //let mut rng = thread_rng();
        //let t = Instant::now();

        let producer: &mut OgProducer = &mut self.0;

        loop {
            let mut erred = false;

            let reqs = &[Record::from_value(topic, item).with_partition(partition)];
            let result = producer.send_all(reqs).unwrap();

            for topic in result.iter() {
                for partition in topic.partition_confirms.iter() {
                    let _p = partition.partition;
                    let _o = match partition.offset {
                        Ok(o) => o,
                        Err(e) => {
                            erred = true;
                            eprintln!("Retrying, {:?}", e);
                            break;
                        }
                    };
                }
            }

            if !erred {
                break;
            }
        }

        //println!(
        //    "Flush: {} bytes, {} seconds",
        //    item.len(),
        //    t.elapsed().as_secs_f64()
        //);
    }
}
