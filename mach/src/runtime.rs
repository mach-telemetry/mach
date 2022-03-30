use lazy_static::*;
use tokio::runtime::{Builder, Runtime};

lazy_static! {
    //pub static ref POOL: ThreadPool = ThreadPool::new().unwrap();
    pub static ref RUNTIME: Runtime = Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    //pub static ref BLOCKING_RUNTIME: Runtime = Builder::new_multi_thread()
    //    .enable_all()
    //    .build()
    //    .unwrap();
}
