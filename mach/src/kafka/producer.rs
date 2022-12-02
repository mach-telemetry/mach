use crate::{constants::*, kafka::KafkaEntry};
use kafka::{
    client::RequiredAcks,
    producer::{Producer as OgProducer, Record},
};
use std::{
    ops::{Deref, DerefMut},
    time::Duration,
};

pub struct Producer(OgProducer);

impl Deref for Producer {
    type Target = OgProducer;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Producer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Producer {
    pub fn new() -> Self {
        let bootstraps = BOOTSTRAPS.split(',').map(String::from).collect();
        let client = OgProducer::from_hosts(bootstraps)
            .with_ack_timeout(Duration::from_millis(10_000))
            .with_required_acks(RequiredAcks::All)
            .create()
            .unwrap();
        Self(client)
    }

    pub fn send(&mut self, partition: i32, item: &[u8]) -> KafkaEntry {
        let mut start = 0;
        //let t = Instant::now();

        let producer: &mut OgProducer = &mut self.0;

        let mut items = Vec::with_capacity(5);

        while start < item.len() {
            let end = item.len().min(start + MAX_MSG_SZ);

            loop {
                let mut erred = false;

                let reqs =
                    &[Record::from_value(TOPIC, &item[start..end]).with_partition(partition)];

                let result = producer.send_all(reqs).unwrap();

                for topic in result.iter() {
                    for partition in topic.partition_confirms.iter() {
                        let p = partition.partition;
                        let o = match partition.offset {
                            Ok(o) => o,
                            Err(_) => {
                                erred = true;
                                std::thread::sleep(Duration::from_millis(500));
                                eprintln!("Retrying");
                                break;
                            }
                        };
                        items.push((p, o))
                    }
                }

                if !erred {
                    start = end;
                    break;
                }
            }
        }
        //let elapsed = t.elapsed();
        //let flush_rate = <f64 as num::NumCast>::from(item.len()).unwrap() / elapsed.as_secs_f64();
        //println!(
        //    "Flush: {} bytes, {} secs, {} rate, iters {}, retries: {}",
        //    item.len(),
        //    t.elapsed().as_secs_f64(),
        //    flush_rate,
        //    num_iters,
        //    num_retries
        //);

        //TOTAL_MB_WRITTEN.fetch_add(item.len(), SeqCst);
        KafkaEntry::from(items)
    }
}
