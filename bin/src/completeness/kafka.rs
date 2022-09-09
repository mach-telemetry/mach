use crate::completeness::{Sample, SampleOwned, WriterGroup, COUNTERS};
use crate::kafka_utils;
use crate::utils::timestamp_now_micros;
use crossbeam_channel::{bounded, Receiver};
use kafka::{
    client::{FetchOffset, KafkaClient},
    consumer::Consumer,
};
use kafka_utils::{make_topic, KafkaTopicOptions};
use lzzzz::lz4;
use mach::id::SeriesId;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

pub fn decompress_kafka_msg(
    msg: &[u8],
    buffer: &mut [u8],
) -> (u64, u64, Vec<SampleOwned<SeriesId>>) {
    let start = u64::from_be_bytes(msg[0..8].try_into().unwrap());
    let end = u64::from_be_bytes(msg[8..16].try_into().unwrap());
    let original_sz = usize::from_be_bytes(msg[msg.len() - 8..msg.len()].try_into().unwrap());
    let sz = lz4::decompress(&msg[16..msg.len() - 8], &mut buffer[..original_sz]).unwrap();
    let data: Vec<SampleOwned<SeriesId>> = bincode::deserialize(&buffer[..sz]).unwrap();
    (start, end, data)
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
            let (_start, end, _data) = decompress_kafka_msg(msg.value, buffer.as_mut_slice());
            max_ts = max_ts.max(end as usize);
        }
    }
    max_ts
}

pub fn init_kafka_consumer(kafka_bootstraps: &'static str, kafka_topic: &'static str) {
    loop {
        std::thread::spawn(move || {
            let max_ts = get_last_kafka_timestamp(kafka_topic, kafka_bootstraps);
            let now: usize = timestamp_now_micros().try_into().unwrap();
            //println!("max ts: {}, age: {}", max_ts, now - max_ts);
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
    receiver: Receiver<(u64, u64, Vec<Sample<SeriesId>>)>,
) {
    let mut producer = kafka_utils::Producer::new(kafka_bootstraps);
    while let Ok(data) = receiver.recv() {
        let (start, end, data) = data;
        let bytes = bincode::serialize(&data).unwrap();
        let mut compressed: Vec<u8> = Vec::new();
        compressed.extend_from_slice(&start.to_be_bytes()[..]);
        compressed.extend_from_slice(&end.to_be_bytes()[..]);
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
