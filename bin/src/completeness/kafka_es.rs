use crate::completeness::{kafka::init_kafka, WriterGroup};
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
    init_kafka(kafka_bootstraps, kafka_topic, num_writers, kafka_topic_opts)
}
