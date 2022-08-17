#[allow(dead_code)]
mod bytes_server;
mod completeness;
mod elastic;
mod kafka_utils;
#[allow(dead_code)]
mod prep_data;
#[allow(dead_code)]
mod snapshotter;
mod utils;
use crate::completeness::{Sample, SampleOwned, Writer, COUNTERS, kafka::decompress_kafka_msg};

use mach::id::SeriesId;
use regex::Regex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage, Message};


fn main() {
    let id = SeriesId(20201894126936780);
    let now: u128 = micros_from_epoch();
    let last_5: u128 = now - Duration::from_secs(5*60).as_micros();
    let now: u64 = now.try_into().unwrap();
    let last_5: u64 = last_5.try_into().unwrap();

    let mut consumer =
        Consumer::from_hosts(vec!["localhost:9093".to_owned(),"localhost:9094".to_owned(),"localhost:9095".to_owned()])
        .with_topic_partitions("kafka-completeness-bench".to_owned(), &[0, 1, 2])
        .with_fallback_offset(FetchOffset::Earliest)
        .with_group("my-group".to_owned())
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .with_fetch_max_bytes_per_partition(5_000_000)
        .create()
        .unwrap();
    let mut buf = vec![0u8; 500_000_000];
    loop {
        for ms in consumer.poll().unwrap().iter() {
            for m in ms.messages() {
                let samples = decompress_kafka_msg(m, &mut buf[..]);
                println!("{:?}", samples.len());
            }
        }
    }
}

fn micros_from_epoch() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
}
