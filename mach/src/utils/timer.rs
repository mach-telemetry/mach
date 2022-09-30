use ref_thread_local::{ref_thread_local, Ref, RefThreadLocal};
use std::collections::HashMap;
pub use std::time::{Duration, Instant};

ref_thread_local! {
    static managed MAP: HashMap<String, Duration> = HashMap::new();
}

pub struct ThreadLocalTimer<'a> {
    instant: Instant,
    key: &'a str,
}

impl<'a> ThreadLocalTimer<'a> {
    pub fn new(key: &'a str) -> Self {
        Self {
            instant: Instant::now(),
            key,
        }
    }

    pub fn reset() {
        MAP.borrow_mut().clear();
    }

    pub fn timers() -> ThreadLocalTimers {
        ThreadLocalTimers::new()
    }
}

impl<'a> Drop for ThreadLocalTimer<'a> {
    fn drop(&mut self) {
        let dur = self.instant.elapsed();
        *MAP.borrow_mut()
            .entry(self.key.into())
            .or_insert_with(|| Duration::from_secs(0)) += dur;
    }
}

#[derive(Debug)]
pub struct ThreadLocalTimers {
    map: Ref<'static, HashMap<String, Duration>>,
}

impl ThreadLocalTimers {
    fn new() -> Self {
        Self { map: MAP.borrow() }
    }
}

impl std::ops::Deref for ThreadLocalTimers {
    type Target = Ref<'static, HashMap<String, Duration>>;
    fn deref(&self) -> &Self::Target {
        &self.map
    }
}
