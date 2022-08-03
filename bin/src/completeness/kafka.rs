use crate::completeness::{Sample, SampleOwned, Writer, COUNTERS};
use crate::kafka_utils;
use crate::utils::timestamp_now_micros;
use crossbeam_channel::{bounded, Receiver};
use kafka::{
    client::{FetchOffset, KafkaClient},
    consumer::{Consumer, Message},
};
use lzzzz::lz4;
use mach::id::SeriesId;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

pub fn decompress_kafka_msg(msg: &Message, buffer: &mut [u8]) -> Vec<SampleOwned<SeriesId>> {
    let original_sz = usize::from_be_bytes(
        msg.value[msg.value.len() - 8..msg.value.len()]
            .try_into()
            .unwrap(),
    );
    let sz = lz4::decompress(
        &msg.value[..msg.value.len() - 8],
        &mut buffer[..original_sz],
    )
    .unwrap();
    let data: Vec<SampleOwned<SeriesId>> = bincode::deserialize(&buffer[..sz]).unwrap();
    data
}

pub fn get_last_kafka_timestamp(topic: &str, bootstraps: &str) -> usize {
    let mut client = KafkaClient::new(bootstraps.split(',').map(String::from).collect());
    client.load_metadata_all().unwrap();

    // move client to latest offsets
    let fetch_offset = FetchOffset::Latest;
    let offsets = client.fetch_offsets(&[topic], fetch_offset).unwrap();
    let mut consumer = Consumer::from_client(client)
        .with_topic(String::from(topic))
        .with_fetch_max_bytes_per_partition(10_000_000)
        .create()
        .unwrap();
    for (_topic, partition_offsets) in offsets.iter() {
        for po in partition_offsets {
            consumer
                .consume_message(topic, po.partition, po.offset)
                .unwrap();
        }
    }

    let mut buffer = vec![0u8; 500_000_000];
    let mut max_ts = 0;
    for set in consumer.poll().unwrap().iter() {
        let _p = set.partition();
        for msg in set.messages().iter() {
            let data = decompress_kafka_msg(msg, buffer.as_mut_slice());
            let ts = data.last().unwrap().1 as usize;
            max_ts = max_ts.max(ts);
        }
    }
    max_ts
}

pub fn init_kafka_consumer(kafka_bootstraps: &'static str, kafka_topic: &'static str) {
    loop {
        std::thread::spawn(move || {
            let max_ts = get_last_kafka_timestamp(kafka_topic, kafka_bootstraps);
            let now: usize = timestamp_now_micros().try_into().unwrap();
            println!("max ts: {}, age: {}", max_ts, now - max_ts);
            if max_ts > 0 {
                COUNTERS.data_age.store(now - max_ts, SeqCst);
            }
        });
        thread::sleep(Duration::from_secs(1));
    }
}

pub fn kafka_writer(
    kafka_bootstraps: &'static str,
    kafka_topic: &'static str,
    barrier: Arc<Barrier>,
    receiver: Receiver<Vec<Sample<SeriesId>>>,
) {
    let mut producer = kafka_utils::Producer::new(kafka_bootstraps);
    while let Ok(data) = receiver.recv() {
        let bytes = bincode::serialize(&data).unwrap();
        let mut compressed: Vec<u8> = Vec::new();
        lz4::compress_to_vec(bytes.as_slice(), &mut compressed, lz4::ACC_LEVEL_DEFAULT).unwrap();
        compressed.extend_from_slice(&bytes.len().to_be_bytes()[..]); // Size of uncompressed bytes
        producer.send(kafka_topic, 0, compressed.as_slice());
        COUNTERS.samples_written.fetch_add(data.len(), SeqCst);
    }
    barrier.wait();
}

pub fn init_kafka(
    kafka_bootstraps: &'static str,
    kafka_topic: &'static str,
    num_writers: usize,
) -> Writer<SeriesId> {
    kafka_utils::make_topic(&kafka_bootstraps, &kafka_topic);
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