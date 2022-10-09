use crate::constants::*;
use num::*;
use rand::Rng;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
    let mut bytes_written = vec![0; len];
    let mut bytes_to_kafka = vec![0; len];
    let mut msgs_to_kafka = vec![0; len];

    let mut counter = 0;

    thread::sleep(Duration::from_secs(10));
    let mut last_kafka_check_time = Instant::now();
    let mut last_kafka_check_bytes = 0;
    loop {
        thread::sleep(Duration::from_secs(1));

        let idx = counter % len;
        counter += 1;

        let current_workload_rate = COUNTERS.current_workload_rate();

        samples_generated[idx] = COUNTERS.samples_generated();
        samples_dropped[idx] = COUNTERS.samples_dropped();

        bytes_generated[idx] = COUNTERS.bytes_generated();
        bytes_written[idx] = COUNTERS.bytes_written();

        bytes_to_kafka[idx] = COUNTERS.bytes_written_to_kafka();
        msgs_to_kafka[idx] = COUNTERS.messages_written_to_kafka();

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

            let div = |num: usize, den: usize| -> f64 {
                let num: f64 = <f64 as NumCast>::from(num).unwrap();
                let den: f64 = <f64 as NumCast>::from(den).unwrap();
                num / den
            };

            let samples_generated_delta = max_min_delta(&samples_generated);
            let samples_dropped_delta = max_min_delta(&samples_dropped);
            let samples_completeness = 1. - div(samples_dropped_delta, samples_generated_delta);

            let bytes_generated_delta = max_min_delta(&bytes_generated);
            let bytes_written_delta = max_min_delta(&bytes_written);
            let bytes_completeness = div(bytes_written_delta, bytes_generated_delta);

            let bytes_to_kafka_delta = max_min_delta(&bytes_to_kafka);
            let msgs_to_kafka_delta = max_min_delta(&msgs_to_kafka);
            let bytes_per_msg = div(bytes_to_kafka_delta, msgs_to_kafka_delta);

            let bytes_since = bytes_to_kafka[idx] - last_kafka_check_bytes;
            let megabytes_since = <f64 as NumCast>::from(bytes_since).unwrap() / 1_000_000.;
            let mbps = megabytes_since / last_kafka_check_time.elapsed().as_secs_f64();
            last_kafka_check_bytes = bytes_to_kafka[idx];
            last_kafka_check_time = Instant::now();

            //let samples_completeness = samples_completeness.iter().sum::<f64>() / denom;
            //let bytes_completeness = bytes_completeness.iter().sum::<f64>() / denom;
            //let bytes_rate = bytes_rate.iter().sum::<f64>() / denom;
            //print!("Sample completeness: {:.2}, ", samples_completeness);
            print!("Current time: {:?}, ", chrono::prelude::Utc::now());
            print!("Current workload rate: {}, ", current_workload_rate);
            print!("Samples generated: {}, ", samples_generated_delta);
            print!("Samples dropped: {}, ", samples_dropped_delta);
            print!("Samples completeness: {:.2}, ", samples_completeness);
            //print!("mbps to kafka: {:.2}, ", mbps);
            //print!("average bytes per msg: {:.2}, ", bytes_per_msg);
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

pub struct RemoteNotifier<A: ToSocketAddrs> {
    remote: A,
}

impl<A> RemoteNotifier<A>
where
    A: ToSocketAddrs,
{
    pub fn new(remote: A) -> Self {
        Self { remote }
    }

    pub fn notify(self) {
        match TcpStream::connect(self.remote) {
            Ok(mut conn) => conn.write_all(&[8; 16]).unwrap(),
            Err(_) => println!("No reachable notification receiver"),
        }
    }
}

pub struct NotificationReceiver {
    listener: TcpListener,
}

impl NotificationReceiver {
    pub fn new(port: u16) -> Self {
        let listener = TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], port))).unwrap();
        Self { listener }
    }

    pub fn wait(&mut self) {
        let (mut stream, _) = self.listener.accept().unwrap();
        let mut buf = [0; 16];
        stream.read_exact(&mut buf).unwrap();
        assert_eq!(buf, [8; 16]);
    }
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
