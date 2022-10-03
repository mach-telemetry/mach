use rand::Rng;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use crate::constants::*;
use std::sync::{Arc, Barrier};
use std::thread;
use num::*;

pub fn stats_printer() -> Arc<Barrier> {
    let barrier = Arc::new(Barrier::new(2));
    let barrier2 = barrier.clone();
    thread::spawn(move || inner_stats_printer(barrier2));
    barrier
}

fn inner_stats_printer(start_barrier: Arc<Barrier>) {
    start_barrier.wait();

    let interval = PARAMETERS.print_interval_seconds as usize;
    let len = interval * 2;

    let mut samples_generated = vec![0; len];
    let mut samples_dropped = vec![0; len];
    let mut bytes_generated = vec![0; len];
    let mut bytes_dropped = vec![0; len];

    let mut counter = 0;

    thread::sleep(Duration::from_secs(10));
    loop {
        thread::sleep(Duration::from_secs(1));

        let idx = counter % len;
        counter += 1;

        samples_generated[idx] = COUNTERS.samples_generated();
        samples_dropped[idx] = COUNTERS.samples_dropped();
        bytes_generated[idx] = COUNTERS.bytes_generated();
        bytes_dropped[idx] = COUNTERS.bytes_dropped();

        if counter % interval == 0 {
            let max_min_delta = |a: &[usize]| -> usize {
                let mut min = usize::MAX;
                let mut max = 0;
                for idx in 0..a.len() {
                    min = min.min(a[idx]);
                    max = max.max(a[idx]);
                }
                max - min
            };

            let percent = |num: usize, den: usize| -> f64 {
                let num: f64 = <f64 as NumCast>::from(num).unwrap();
                let den: f64 = <f64 as NumCast>::from(den).unwrap();
                num / den
            };

            let samples_generated_delta = max_min_delta(&samples_generated);
            let samples_dropped_delta = max_min_delta(&samples_dropped);
            let samples_completeness = 1. - percent(samples_dropped_delta, samples_generated_delta);

            let bytes_generated_delta = max_min_delta(&bytes_generated);
            let bytes_dropped_delta = max_min_delta(&bytes_dropped);
            let bytes_completeness = 1. - percent(bytes_dropped_delta, bytes_generated_delta);

            //let samples_completeness = samples_completeness.iter().sum::<f64>() / denom;
            //let bytes_completeness = bytes_completeness.iter().sum::<f64>() / denom;
            //let bytes_rate = bytes_rate.iter().sum::<f64>() / denom;
            print!("Sample completeness: {:.2}, ", samples_completeness);
            print!("Bytes completeness: {:.2}, ", bytes_completeness);
            println!("");
        }
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
