use crate::completeness::{Sample, Writer, COUNTERS};
use crossbeam_channel::{bounded, Receiver};
use lazy_static::lazy_static;
use mach::{
    id::{SeriesId, SeriesRef},
    series::Series,
    tsdb::Mach,
    writer::Writer as MachWriter,
    writer::WriterConfig,
};
use std::sync::atomic::Ordering::SeqCst;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

lazy_static! {
    pub static ref MACH: Arc<Mutex<Mach>> = Arc::new(Mutex::new(Mach::new()));
    pub static ref MACH_WRITER: Arc<Mutex<MachWriter>> = {
        let mach = MACH.clone(); // ensure MACH is initialized (prevent deadlock)
        let writer_config = WriterConfig {
            active_block_flush_sz: 1_000_000,
        };
        let mut guard = mach.lock().unwrap();
        Arc::new(Mutex::new(guard.add_writer(writer_config).unwrap()))
    };
}

fn micros_from_epoch() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
}

#[allow(dead_code)]
fn mach_query(series: Series) -> Option<usize> {
    let snapshot = series.snapshot();
    let mut snapshot = snapshot.into_iterator();
    snapshot.next_segment().unwrap();
    let seg = snapshot.get_segment();
    let mut timestamps = seg.timestamps().iterator();
    let ts: usize = timestamps.next_timestamp()? as usize;
    let now: usize = micros_from_epoch().try_into().unwrap();
    Some(now - ts)
}

pub fn init_mach_querier(series_id: SeriesId) {
    let snapshotter = MACH.lock().unwrap().init_snapshotter();
    let snapshotter_id = snapshotter.initialize_snapshotter(
        series_id,
        Duration::from_millis(500),
        Duration::from_secs(300),
    );
    thread::sleep(Duration::from_secs(5));
    loop {
        let now: usize = micros_from_epoch().try_into().unwrap();
        let offset = snapshotter.get(snapshotter_id).unwrap();
        let mut snapshot = offset.load().into_iterator();
        snapshot.next_segment().unwrap();
        let seg = snapshot.get_segment();
        let mut timestamps = seg.timestamps().iterator();
        let ts: usize = timestamps.next_timestamp().unwrap().try_into().unwrap();
        COUNTERS.data_age.store(now - ts, SeqCst);
        thread::sleep(Duration::from_secs(1));
    }
}

fn mach_writer(barrier: Arc<Barrier>, receiver: Receiver<(u64, u64, Vec<Sample<SeriesRef>>)>) {
    let mut writer_guard = MACH_WRITER.lock().unwrap();
    let writer = &mut *writer_guard;
    while let Ok(data) = receiver.recv() {
        let (start, end, data) = data;
        let mut raw_sz = 0;
        for item in data.iter() {
            'push_loop: loop {
                if writer.push(item.0, item.1, item.2).is_ok() {
                    //println!("Data size: {}", item.2[0].as_bytes().len());
                    raw_sz += item.2[0].as_bytes().len();
                    break 'push_loop;
                }
            }
        }
        COUNTERS.raw_data_size.fetch_add(raw_sz, SeqCst);
        COUNTERS.samples_written.fetch_add(data.len(), SeqCst);
        COUNTERS.samples_dropped.fetch_add(0, SeqCst);
    }
    barrier.wait();
}

pub fn init_mach() -> Writer<SeriesRef> {
    let (tx, rx) = bounded(1);
    let barrier = Arc::new(Barrier::new(2));

    {
        let barrier = barrier.clone();
        let rx = rx.clone();
        thread::spawn(move || {
            mach_writer(barrier, rx);
        });
    }

    Writer {
        sender: tx,
        barrier,
    }
}
