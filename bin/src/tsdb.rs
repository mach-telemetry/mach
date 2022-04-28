pub mod rpc {
    tonic::include_proto!("mach_rpc"); // The string specified here must match the proto package name
}
mod tag_index;
mod mach_tsdb;
mod none_tsdb;

use rpc::tsdb_service_server::{TsdbServiceServer};
use lazy_static::lazy_static;
use tonic::transport::Server;
use clap::Parser;

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc,
    },
    time::Duration,
};
use mach_tsdb::MachTSDB;
use none_tsdb::NoneTSDB;

#[derive(Parser, Debug)]
struct Args {
    #[clap(short, long, default_value_t = String::from("mach"))]
    tsdb: String,

    #[clap(short, long, default_value_t = String::from("127.0.0.1"))]
    addr: String,

    #[clap(short, long, default_value_t = String::from("50050"))]
    port: String,
}

lazy_static! {
    pub static ref COUNTER: Arc<AtomicUsize> = {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();
        tokio::spawn(async move {
            let mut last = 0;
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let cur = counter_clone.load(SeqCst);
                println!("received {} / sec", cur - last);
                last = cur;
            }
        });
        counter
    };
}

pub fn increment_sample_counter(v: usize) {
    COUNTER.fetch_add(v, SeqCst);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    println!("Args: {:?}", args);
    let mut addr = String::from(args.addr.as_str());
    addr.push(':');
    addr.push_str(args.port.as_str());
    let addr = addr.parse()?;
    let _counter = COUNTER.load(SeqCst); // init counter

    match args.tsdb.as_str() {
        "mach" => {
            let tsdb = MachTSDB::new();
            Server::builder()
                .add_service(TsdbServiceServer::new(tsdb))
                .serve(addr)
                .await?;
        },
        "none" => {
            let tsdb = NoneTSDB::new();
            Server::builder()
                .add_service(TsdbServiceServer::new(tsdb))
                .serve(addr)
                .await?;
        }
        _ => {
            unimplemented!()
        }
    }

    Ok(())
}
