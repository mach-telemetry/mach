#[allow(dead_code)]
mod bytes_server;
//mod prep_data;
#[allow(dead_code)]
mod snapshotter;

use mach::id::SeriesId;
use regex::Regex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn get_most_recent_timestamp() {}

fn main() {
    mach::utils::kafka::init_kafka_consumer();
    std::thread::sleep(Duration::from_secs(5 * 60));
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

    //let sleep = Duration::from_secs(7 * 60);
    //println!("sleeping for {:?}", sleep);
    //std::thread::sleep(sleep);

    // Setup query
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut client = runtime.block_on(snapshotter::SnapshotClient::new());
    let id = SeriesId(20201894126936780);
    let interval = Duration::from_secs_f64(0.5);
    let timeout = Duration::from_secs(60 * 60);

    let snapshotter_id = runtime
        .block_on(client.initialize(id, interval, timeout))
        .unwrap();
    println!("snapshotter id: {:?}", snapshotter_id);

    let now: u128 = micros_from_epoch();
    let last_5: u128 = now - Duration::from_secs(5 * 60).as_micros();

    let now: u64 = now.try_into().unwrap();
    let last_5: u64 = last_5.try_into().unwrap();

    let mut count = 0;
    let total_start = Instant::now();
    let mut query_execution_time = Duration::from_secs(0);
    println!("Waiting for data to be available");
    loop {
        let snapshot_id = runtime.block_on(client.get(snapshotter_id)).unwrap();
        let mut snapshot = snapshot_id.load().into_iterator();
        //loop {
        snapshot.next_segment().unwrap();
        let seg = snapshot.get_segment();
        let mut timestamps = seg.timestamps().iterator();
        let ts = timestamps.next().unwrap();
        if ts > now {
            break;
        }
    }

    println!("Executing query");
    let mut query_execution_start = Instant::now();
    let snapshot_id = runtime.block_on(client.get(snapshotter_id)).unwrap();
    let mut snapshot = snapshot_id.load().into_iterator();
    println!("Range: {} {}", now, last_5);
    let mut result = Vec::new();
    'outer: loop {
        if snapshot.next_segment().is_none() {
            break;
        }
        let seg = snapshot.get_segment();
        let mut timestamps = seg.timestamps().iterator();
        let mut count = 0;
        for (i, ts) in timestamps.enumerate() {
            result.push(ts);
            if ts < last_5 {
                break 'outer;
            }
        }
    }
    println!(
        "Last timestamps: {} {:?}",
        result.len(),
        &result[result.len() - 2..]
    );
    let query_execution_time = query_execution_start.elapsed();
    let total_time = total_start.elapsed();
    println!(
        "Total Time: {:?}, Query Execution Time: {:?}",
        total_time.as_secs_f64(),
        query_execution_time.as_secs_f64()
    );
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

    // Query the number of samples from a source for the past 5 minutes

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
