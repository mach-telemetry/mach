use crate::completeness::{kafka::kafka_writer, WriterGroup};
use crate::kafka_utils::{make_topic, KafkaTopicOptions};
use crossbeam_channel::bounded;
use mach::id::SeriesId;
use std::sync::{Arc, Barrier};
use std::thread;

pub fn init_kafka_es(
    kafka_bootstraps: &'static str,
    kafka_topic: &'static str,
    num_writers: usize,
    kafka_topic_opts: KafkaTopicOptions,
) -> WriterGroup<SeriesId> {
    make_topic(&kafka_bootstraps, &kafka_topic, kafka_topic_opts);
    let barrier = Arc::new(Barrier::new(num_writers + 1));
    let mut senders = Vec::with_capacity(num_writers);

    for _ in 0..num_writers {
        let barrier = barrier.clone();
        let (tx, rx) = bounded(1);
        senders.push(tx);
        thread::spawn(move || {
            kafka_writer(kafka_bootstraps, kafka_topic, barrier, rx);
        });
    }

    WriterGroup { senders, barrier }
}
