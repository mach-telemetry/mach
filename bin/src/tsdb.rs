pub mod rpc {
    tonic::include_proto!("mach_rpc"); // The string specified here must match the proto package name
}
mod file_tsdb;
mod mach_tsdb;
mod none_tsdb;
mod sample;
mod tag_index;

use clap::Parser;
use lazy_static::lazy_static;
use rpc::tsdb_service_server::TsdbServiceServer;
use tonic::transport::Server;

use file_tsdb::FileTSDB;
use mach_tsdb::MachTSDB;
use none_tsdb::NoneTSDB;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc,
    },
    time::Duration,
};

#[derive(Parser, Debug)]
struct Args {
    #[clap(short, long, default_value_t = String::from("mach"))]
    tsdb: String,

    #[clap(short, long, default_value_t = String::from("127.0.0.1"))]
    addr: String,

    #[clap(short, long, default_value_t = String::from("50050"))]
    port: String,

    #[clap(short, long)]
    path: Option<String>,
}

lazy_static! {
    pub static ref COUNTER: Arc<AtomicUsize> = {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();
        let mut data = [0; 5];
        tokio::spawn(async move {
            let mut last = 0;
            for i in 0.. {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let idx = i % 5;
                data[idx] = counter_clone.load(SeqCst);
                let max = data.iter().max().unwrap();
                let min = data.iter().min().unwrap();
                let rate = (max - min) / 5;
                println!("received {} / sec", rate);
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
        }
        "none" => {
            let tsdb = NoneTSDB::new();
            Server::builder()
                .add_service(TsdbServiceServer::new(tsdb))
                .serve(addr)
                .await?;
        }
        "file" => {
            let p = args.path.as_deref().unwrap();
            let tsdb = FileTSDB::new(p);
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
