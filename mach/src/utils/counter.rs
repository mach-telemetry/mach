use ref_thread_local::{ref_thread_local, Ref, RefThreadLocal};
use std::collections::{hash_map, HashMap};
use std::convert::TryInto;

ref_thread_local! {
    static managed MAP: HashMap<String, usize> = HashMap::new();
}

pub struct ThreadLocalCounter<'a> {
    key: &'a str,
}

impl<'a> ThreadLocalCounter<'a> {
    pub fn new(key: &'a str) -> Self {
        MAP.borrow_mut().entry(key.into()).or_insert(0);
        Self { key }
    }

    pub fn reset() {
        MAP.borrow_mut().clear();
    }

    pub fn increment(&self, v: usize) {
        *MAP.borrow_mut().get_mut(&String::from(self.key)).unwrap() += v;
    }

    pub fn counters() -> ThreadLocalCounters {
        ThreadLocalCounters::new()
    }
}

#[derive(Debug)]
pub struct ThreadLocalCounters {
    map: Ref<'static, HashMap<String, usize>>,
}

impl ThreadLocalCounters {
    fn new() -> Self {
        Self { map: MAP.borrow() }
    }
}

impl std::ops::Deref for ThreadLocalCounters {
    type Target = Ref<'static, HashMap<String, usize>>;
    fn deref(&self) -> &Self::Target {
        &self.map
    }
}
