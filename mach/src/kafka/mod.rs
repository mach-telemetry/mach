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

mod kafka_entry;
mod producer;
pub use kafka_entry::KafkaEntry;
pub use producer::Producer;

use crate::constants::*;
use log::info;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
};
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::time::Duration;

static INITED: AtomicBool = AtomicBool::new(false);

pub fn init() {
    if !INITED.load(SeqCst) {
        info!(
            "Initing Kafka Connection\nbootstraps: {}\ntopic: {}\nreplicas:{}\npartitions:{}",
            BOOTSTRAPS, TOPIC, REPLICAS, PARTITIONS
        );
        make_topic();
        INITED.store(true, SeqCst);
    }
}

pub fn make_topic() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let client: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", BOOTSTRAPS)
        .create()
        .unwrap();
    let admin_opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(3)));
    let replicas = format!("{}", REPLICAS);
    let topics = &[NewTopic {
        name: TOPIC,
        num_partitions: PARTITIONS,
        replication: TopicReplication::Fixed(REPLICAS),
        config: vec![("min.insync.replicas", replicas.as_str())],
    }];
    rt.block_on(client.create_topics(topics, &admin_opts))
        .unwrap();
}
