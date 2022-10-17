#[allow(dead_code)]
mod constants;
#[allow(dead_code)]
mod utils;

use std::time::{Duration, Instant};

use mach::{
    compression::{CompressFn, Compression},
    constants::*,
    id::{SeriesId, SeriesRef},
    sample::SampleType,
    series::{FieldType, SeriesConfig},
    tsdb::{self, Mach},
    writer::Writer,
    writer::WriterConfig,
};
use utils::timestamp_now_micros;

fn bench_timestamp_compression(mach: &mut Mach, writer: &mut Writer) {
    let mut series_id_counter = 0;
    let run_dur = Duration::from_secs(60);

    let mut make_series_cfg = |compression: CompressFn| {
        let cfg = SeriesConfig {
            id: SeriesId(series_id_counter),
            types: vec![FieldType::U64],
            compression: Compression::from(vec![compression]),
            seg_count: 1,
            nvars: 1,
        };
        series_id_counter += 1;
        cfg
    };

    let series_cfg = make_series_cfg(CompressFn::DeltaDelta);
    let delta_delta_id = series_cfg.id;
    mach.add_series_to_writer(series_cfg, writer.id());
    let delta_series_ref = writer.get_reference(delta_delta_id);

    let series_cfg = make_series_cfg(CompressFn::LZ4);
    let lz4_id = series_cfg.id;
    mach.add_series_to_writer(series_cfg, writer.id());
    let lz4_ref = writer.get_reference(lz4_id);

    let mut delta_delta_count = 0;
    let time = Instant::now();
    while time.elapsed() < run_dur {
        'push: loop {
            let ts = timestamp_now_micros();
            let val = vec![SampleType::U64(ts)];
            if writer.push(delta_series_ref, ts, &val).is_ok() {
                delta_delta_count += 1;
                break 'push;
            }
        }
    }

    let mut lz4_count = 0;
    let time = Instant::now();
    while time.elapsed() < run_dur {
        'push: loop {
            let ts = timestamp_now_micros();
            let val = vec![SampleType::U64(ts)];
            if writer.push(lz4_ref, ts, &val).is_ok() {
                lz4_count += 1;
                break 'push;
            }
        }
    }

    println!("delta of delta: pushed {delta_delta_count} samples");
    println!("lz4: pushed {lz4_count} samples");
}

fn main() {
    const DEFAULT_WRITER_CFG: WriterConfig = WriterConfig {
        active_block_flush_sz: 1_000_000,
    };

    let mut mach = Mach::new();
    let mut writer = mach.add_writer(DEFAULT_WRITER_CFG).unwrap();

    bench_timestamp_compression(&mut mach, &mut writer);
}
