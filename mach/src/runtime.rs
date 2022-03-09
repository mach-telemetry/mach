use lazy_static::*;
use tokio::runtime::{Builder, Runtime};

lazy_static! {
    pub static ref RUNTIME: Runtime = Builder::new_multi_thread()
        .enable_all()
        //.worker_threads(4)
        .build()
        .unwrap();
}
