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

mod kafka_utils;

use clap::*;
use crossbeam::channel::{bounded, Receiver, Sender};
use kafka_utils::{make_topic, Producer};
use lazy_static::lazy_static;
use num_format::{Locale, ToFormattedString};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use std::{
    sync::atomic::{AtomicU64, Ordering::SeqCst},
    thread,
    time::{Duration, Instant},
};

lazy_static! {
    static ref BYTES: Box<[u8]> = vec![8u8; 1_000_000].into_boxed_slice();
    static ref ARGS: Args = Args::parse();
    static ref TOPIC_NAMES: Vec<String> = {
        let num_topics = ARGS.kafka_topics;
        assert!(num_topics > 0);
        (0..num_topics).map(|_| random_topic()).collect()
    };
}

#[derive(Parser, Debug, Clone)]
struct Args {
    #[clap(short, long, default_value_t = 4)]
    writer_count: i32,
    #[clap(short, long, default_value_t = 4)]
    kafka_partitions: i32,
    #[clap(short, long, default_value_t = 4)]
    kafka_topics: i32,
    #[clap(short, long, default_value_t = String::from("localhost:9093,localhost:9094,localhost:9095"))]
    bootstrap_servers: String,
    /// Rate in mbps
    #[clap(short, long, default_value_t = 100)]
    rate: u64,
    #[clap(short, long, default_value_t = random_topic())]
    topic: String,
}

fn random_topic() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(7)
        .map(char::from)
        .collect()
}

fn kafka_writer(rx: Receiver<()>) {
    let mut rng = thread_rng();
    let mut producer = Producer::new(ARGS.bootstrap_servers.as_str(), true);
    let payload = &BYTES[..];
    loop {
        if let Ok(x) = rx.try_recv() {
            let topic_idx = rng.gen_range(0..TOPIC_NAMES.len());
            let partition = rng.gen_range(0..ARGS.kafka_partitions);
            producer.send(&TOPIC_NAMES[topic_idx], partition, payload);
        }
    }
}

fn init_kafka_loader() -> Sender<()> {
    let (tx, rx) = bounded(ARGS.rate as usize);
    for _ in 0..ARGS.writer_count {
        let rx = rx.clone();
        thread::spawn(move || kafka_writer(rx));
    }
    tx
}

fn main() {
    let opts = kafka_utils::KafkaTopicOptions {
        num_partitions: ARGS.kafka_partitions,
        num_replicas: 3,
    };

    for topic in TOPIC_NAMES.iter() {
        make_topic(ARGS.bootstrap_servers.as_str(), topic, opts);
    }

    let mut sender = init_kafka_loader();

    let mut counter = 0;
    loop {
        let now = Instant::now();
        for _ in 0..ARGS.rate {
            if sender.try_send(()).is_ok() {
                counter += 1;
            }
        }
        while now.elapsed() < Duration::from_secs(1) {}
        println!("Sent {} mb", counter);
        counter = 0;
    }
}
