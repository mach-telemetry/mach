mod prep_data;
mod bytes_server;
mod snapshotter;

use regex::Regex;
use mach::{
    utils::{random_id, kafka::{BOOTSTRAPS, TOPIC}},
};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
    consumer::{Consumer as RdKConsumer, DefaultConsumerContext, BaseConsumer},
    topic_partition_list::{TopicPartitionList, Offset},
    util::Timeout,
    Message,
};

fn main() {
    //let data: Vec<prep_data::Sample> = prep_data::load_samples("/home/sli/data/train-ticket-data");
    //let re = Regex::new(r"Error").unwrap();
    //for sample in data.iter() {
    //    let span: otlp::trace::v1::Span = bincode::deserialize(&sample.2[0].as_bytes()).unwrap();
    //    for kv in span.events[0].attributes.iter() {
    //        let value = kv.value.as_ref().unwrap().as_str();
    //        println!("{:?} {}", value, re.find(value).is_some());
    //    }
    //    break;
    //}

    let mut consumer: BaseConsumer<DefaultConsumerContext> = ClientConfig::new()
        .set("bootstrap.servers", BOOTSTRAPS)
        .set("group.id", random_id())
        .create().unwrap();
    let mut topic_partition_list = TopicPartitionList::new();
    topic_partition_list.add_partition_offset(TOPIC, 0, Offset::Offset(1)).unwrap();
    topic_partition_list.add_partition_offset(TOPIC, 1, Offset::Offset(123)).unwrap();
    topic_partition_list.add_partition_offset(TOPIC, 2, Offset::Offset(25)).unwrap();
    consumer.assign(&topic_partition_list).unwrap();
    let mut counter = 0;
    for _ in 0..100 {
        match consumer.poll(Timeout::After(std::time::Duration::from_secs(1))) {
            Some(Ok(msg)) => {
                println!("msg.partition {} msg.offset {}", msg.partition(), msg.offset());
            },
            Some(Err(x)) => panic!("{:?}", x),
            None => {}
        }
    }
}
