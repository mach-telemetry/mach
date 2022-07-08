pub mod rpc {
    // The string specified here must match the proto package name
    tonic::include_proto!("mach_rpc");
}

use lazy_static::*;
use rpc::tsdb_service_client::TsdbServiceClient;
use std::collections::HashMap;
use std::sync::{
    mpsc::{channel, Receiver, Sender},
    Arc, Mutex, RwLock,
};
use std::thread;

//use tonic::{Request, Response, Status};
//use rpc::tsdb_service_server::TsdbServiceServer;

const BUFFER_SZ: usize = 8192;

lazy_static! {
    static ref SAMPLE_SENDER: Arc<Mutex<Sender<Sample>>> = {
        let (tx, rx) = channel();
        init_client_worker(rx);
        Arc::new(Mutex::new(tx))
    };
}

// TODO: Some async central loop

type Types = rpc::source::Types;
type Sample = rpc::Sample;
type Value = rpc::Value;

pub struct Source {
    push_metadata: rpc::PushMetadata,
    sender: Sender<Sample>,
}

impl Source {
    pub fn new(tags: HashMap<String, String>, types: Vec<Types>) -> Self {
        let source_registration = rpc::Source {
            tags,
            types: types.iter().copied().map(|x| x.into()).collect(),
        };

        // Todo send registration, receive back PushMetadata
        let push_metadata = rpc::PushMetadata {
            writer: 0,
            reference: 0,
        };
        Self {
            push_metadata,
            sender: (*SAMPLE_SENDER).lock().unwrap().clone(),
        }
    }

    pub fn send(&self, timestamp: u64, values: &[Value]) {
        let sample = Sample {
            push_meta: Some(self.push_metadata.clone()),
            timestamp,
            values: values.into(),
        };
        self.sender.send(sample).unwrap();
    }
}

fn init_client_worker(rx: Receiver<Sample>) {
    thread::spawn(move || {
        let mut samples = Vec::new();
        while let Ok(item) = rx.recv() {
            samples.push(item);
            if samples.len() == BUFFER_SZ {
                let batch = rpc::Batch { samples };
                // TODO: Send to Mach
                samples = Vec::new();
            }
        }
    });
}

struct Client {}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
