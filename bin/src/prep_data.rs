use mach::{
    id::SeriesId,
    id::SeriesRef,
    sample::SampleType,
    series::{FieldType, SeriesConfig},
    compression::{CompressFn, Compression},
    tsdb::Mach,
    writer::{Writer, WriterConfig},
};

use std::{
    fs::File,
    io::prelude::*,
    collections::HashMap,
};

pub type TimeStamp = u64;
pub type Sample = (SeriesId, TimeStamp, Vec<SampleType>);
pub type RegisteredSample = (SeriesRef, TimeStamp, Vec<SampleType>);

fn load_data(path: &str) -> Vec<otlp::OtlpData> {
    println!("Loading data from: {}", path);
    let mut data = Vec::new();
    File::open(path).unwrap().read_to_end(&mut data).unwrap();
    bincode::deserialize(data.as_slice()).unwrap()
}

fn otlp_data_to_samples(data: &[otlp::OtlpData]) -> Vec<Sample> {
    println!("Converting data to samples");
    let data: Vec<mach_otlp::OtlpData> = data
        .iter()
        .map(|x| {
            let mut x: mach_otlp::OtlpData = x.into();
            match &mut x {
                mach_otlp::OtlpData::Spans(x) => x.iter_mut().for_each(|x| x.set_source_id()),
                _ => unimplemented!(),
            }
            x
        })
        .collect();

    let mut samples = Vec::new();

    for entry in data {
        match entry {
            mach_otlp::OtlpData::Spans(spans) => {
                for span in spans {
                    span.get_samples(&mut samples);
                }
            }
            _ => unimplemented!(),
        }
    }

    samples
}

pub fn load_samples(path: &str) -> Vec<Sample> {
    let data = load_data(path);
    otlp_data_to_samples(data.as_slice())
}

pub fn mach_register_samples(
    samples: &[Sample],
    mach: &mut Mach,
    writer: &mut Writer,
) -> Vec<RegisteredSample> {
    let mut refmap: HashMap<SeriesId, SeriesRef> = HashMap::new();

    for (id, _, values) in samples.iter() {
        let id_ref = *refmap.entry(*id).or_insert_with(|| {
            let conf = get_series_config(*id, values.as_slice());
            let (wid, _) = mach.add_series(conf).unwrap();
            let id_ref = writer.get_reference(*id);
            id_ref
        });
    }

    let mut registered_samples = Vec::new();
    for (series_id, ts, values) in samples {
        let series_ref = *refmap.get(&series_id).unwrap();
        registered_samples.push((series_ref, *ts, values.clone()));
    }
    registered_samples
}

fn get_series_config(id: SeriesId, values: &[SampleType]) -> SeriesConfig {
    let mut types = Vec::new();
    let mut compression = Vec::new();
    values.iter().for_each(|v| {
        let (t, c) = match v {
            //SampleType::U32(_) => (FieldType::U32, CompressFn::IntBitpack),
            SampleType::U64(_) => (FieldType::U64, CompressFn::LZ4),
            SampleType::F64(_) => (FieldType::F64, CompressFn::Decimal(3)),
            SampleType::Bytes(_) => (FieldType::Bytes, CompressFn::BytesLZ4),
            //SampleType::BorrowedBytes(_) => (FieldType::Bytes, CompressFn::NOOP),
            _ => unimplemented!(),
        };
        types.push(t);
        compression.push(c);
    });
    let compression = Compression::from(compression);
    let nvars = types.len();
    let conf = SeriesConfig {
        id,
        types,
        compression,
        seg_count: 3,
        nvars,
    };
    conf
}
