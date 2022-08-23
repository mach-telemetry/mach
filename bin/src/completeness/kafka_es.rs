use crate::completeness::{kafka::kafka_writer, Writer};
use crate::kafka_utils::make_topic;
use crossbeam_channel::bounded;
use mach::id::SeriesId;
use std::sync::{Arc, Barrier};
use std::thread;

pub fn init_kafka_es(
    kafka_bootstraps: &'static str,
    kafka_topic: &'static str,
    num_writers: usize,
) -> Writer<SeriesId> {
    make_topic(&kafka_bootstraps, &kafka_topic);
    let (tx, rx) = bounded(1);
    let barrier = Arc::new(Barrier::new(num_writers + 1));

    for _ in 0..num_writers {
        let barrier = barrier.clone();
        let rx = rx.clone();
        thread::spawn(move || {
            kafka_writer(kafka_bootstraps, kafka_topic, barrier, rx);
        });
    }

    Writer {
        sender: tx,
        barrier,
    }
}
