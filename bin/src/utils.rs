use rand::Rng;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use crate::constants::*;
use crossbeam::channel::{bounded, unbounded, Sender, Receiver};

pub fn parameterized_queue<T>() -> (Sender<T>, Receiver<T>) {
    if PARAMETERS.unbounded_queue {
        unbounded()
    } else {
        bounded(1)
    }
}



pub fn timestamp_now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
        .try_into()
        .unwrap()
}

pub struct ExponentialBackoff {
    initial_interval: Duration,
    curr_interval: Duration,
    max_interval: Duration,
    multiplier: usize,
}

impl ExponentialBackoff {
    pub fn new(initial_interval: Duration, max_interval: Duration) -> Self {
        let curr_interval = initial_interval.clone();
        Self {
            initial_interval,
            curr_interval,
            max_interval,
            multiplier: 2,
        }
    }

    pub fn reset(&mut self) {
        self.curr_interval = self.initial_interval;
    }

    pub fn next_backoff(&mut self) -> Duration {
        let mut rng = rand::thread_rng();
        let rand_wait_time_us: u64 = rng
            .gen_range(0..self.initial_interval.as_micros())
            .try_into()
            .unwrap();

        let maybe_next_interval = self.curr_interval * self.multiplier.try_into().unwrap()
            + Duration::from_micros(rand_wait_time_us);

        self.curr_interval = if maybe_next_interval < self.max_interval {
            maybe_next_interval
        } else {
            self.max_interval
        };

        self.curr_interval
    }
}
