use ref_thread_local::{ref_thread_local, Ref, RefThreadLocal};
use std::collections::HashMap;
pub use std::time::{Duration, Instant};

ref_thread_local! {
    static managed MAP: HashMap<String, Duration> = HashMap::new();
}

const TIMER_ON: bool = false;

pub struct ThreadLocalTimer<'a> {
    instant: Option<Instant>,
    key: &'a str,
}

impl<'a> ThreadLocalTimer<'a> {
    pub fn new(key: &'a str) -> Self {
        Self {
            instant: if TIMER_ON { Some(Instant::now()) } else { None },
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
        if let Some(instant) = self.instant {
            let dur = instant.elapsed();
            *MAP.borrow_mut()
                .entry(self.key.into())
                .or_insert_with(|| Duration::from_secs(0)) += dur;
        }
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
