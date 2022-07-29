#[allow(dead_code)]
mod bytes_server;
//mod prep_data;
#[allow(dead_code)]
mod snapshotter;

use mach::id::SeriesId;
use regex::Regex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn main() {
    //let data: Vec<prep_data::Sample> = prep_data::load_samples("/home/sli/data/train-ticket-data");
    //println!("Samples: {}", data.len());
    //let mut count = 0;
    //let re = Regex::new(r"Error").unwrap();
    //let now = std::time::Instant::now();
    //'sample: for sample in data.iter() {
    //    let span: otlp::trace::v1::Span = bincode::deserialize(&sample.2[0].as_bytes()).unwrap();
    //    'event: for event in span.events.iter() {
    //        'kv: for kv in event.attributes.iter() {
    //            if kv.key == "handler" && re.find(kv.value.as_ref().unwrap().as_str()).is_some() {
    //                count += 1;
    //                break 'event;
    //            }
    //        }
    //    }
    //}
    //println!("Count: {:?}", count);
    //println!("Elapsed: {:?}", now.elapsed());

    //let mut consumer: BaseConsumer<DefaultConsumerContext> = ClientConfig::new()
    //    .set("bootstrap.servers", BOOTSTRAPS)
    //    .set("group.id", random_id())
    //    .create().unwrap();
    //let mut topic_partition_list = TopicPartitionList::new();
    //topic_partition_list.add_partition_offset(TOPIC, 0, Offset::Offset(1)).unwrap();
    //topic_partition_list.add_partition_offset(TOPIC, 1, Offset::Offset(123)).unwrap();
    //topic_partition_list.add_partition_offset(TOPIC, 2, Offset::Offset(25)).unwrap();
    //consumer.assign(&topic_partition_list).unwrap();
    //let mut counter = 0;
    //for _ in 0..100 {
    //    match consumer.poll(Timeout::After(std::time::Duration::from_secs(1))) {
    //        Some(Ok(msg)) => {
    //            println!("msg.partition {} msg.offset {}", msg.partition(), msg.offset());
    //        },
    //        Some(Err(x)) => panic!("{:?}", x),
    //        None => {}
    //    }
    //}

    // Setup query
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut client = runtime.block_on(snapshotter::SnapshotClient::new());
    let id = SeriesId(4560055620737106128);
    let interval = Duration::from_secs_f64(0.5);
    let timeout = Duration::from_secs(300);
    let snapshotter_id = runtime
        .block_on(client.initialize(id, interval, timeout))
        .unwrap();
    println!("snapshotter id: {:?}", snapshotter_id);
    //let mut kafka_client = Client::new(BOOTSTRAPS);
    //let consumer_offset = ConsumerOffset::Latest;
    //let mut consumer = BufferedConsumer::new(BOOTSTRAPS, TOPIC, consumer_offset);

    // Freshness query
    //loop {
    //    let start: usize = micros_from_epoch().try_into().unwrap();
    //    let snapshot_id = runtime.block_on(client.get(snapshotter_id)).unwrap();
    //    let mut snapshot = snapshot_id.load(&mut kafka_client).into_iterator(&mut consumer);
    //    snapshot.next_segment().unwrap();
    //    let seg = snapshot.get_segment();
    //    let mut timestamps = seg.timestamps().iterator();
    //    let ts: usize = timestamps.next_timestamp().unwrap().try_into().unwrap();
    //    let end: usize = micros_from_epoch().try_into().unwrap();
    //    let duration = Duration::from_micros((end - start) as u64);
    //    let age = Duration::from_micros((start - ts) as u64);
    //    println!("snapshot id: {:?}, query latency: {:?}, data age: {:?}", snapshot_id, duration, age);
    //    std::thread::sleep(Duration::from_secs(1));
    //}

    // Search query
    let _re = Regex::new(r"Error").unwrap();
    let _start: usize = micros_from_epoch().try_into().unwrap();
    let snapshot_id = runtime.block_on(client.get(snapshotter_id)).unwrap();
    let mut snapshot = snapshot_id.load().into_iterator();
    //loop {
    snapshot.next_segment().unwrap();
    let seg = snapshot.get_segment();
    let timestamps = seg.timestamps().iterator();
    let field = seg.field(0).iterator();
    println!("HERE");
    for (_idx, (ts, f)) in timestamps.zip(field).enumerate() {
        let span = mach_otlp::trace::v1::Span::from(&f);
        println!("TS {:?}", ts);
        println!("Span: {:?}", span);
    }
    //let ts: usize = timestamps.next_timestamp().unwrap().try_into().unwrap();
    //}
    //let end: usize = micros_from_epoch().try_into().unwrap();
    //let duration = Duration::from_micros((end - start) as u64);
    //let age = Duration::from_micros((start - ts) as u64);
    //println!("snapshot id: {:?}, query latency: {:?}, data age: {:?}", snapshot_id, duration, age);
}

fn micros_from_epoch() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
}
