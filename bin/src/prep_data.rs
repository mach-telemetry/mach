use elasticsearch::http::request::JsonBody;
use mach::{
    compression::{CompressFn, Compression},
    id::SeriesId,
    id::SeriesRef,
    sample::SampleType,
    series::{FieldType, SeriesConfig},
    tsdb::Mach,
    writer::Writer,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs::File, io::prelude::*};

pub type TimeStamp = u64;
pub type Sample = (SeriesId, TimeStamp, Vec<SampleType>);
pub type RegisteredSample = (SeriesRef, TimeStamp, Vec<SampleType>);

fn load_data(path: &str) -> Vec<otlp::OtlpData> {
    println!("Loading data from: {}", path);
    let mut data = Vec::new();
    File::open(path).unwrap().read_to_end(&mut data).unwrap();
    bincode::deserialize(data.as_slice()).unwrap()
}

fn otlp_data_to_samples(data: &[otlp::OtlpData]) -> HashMap<SeriesId, Vec<(u64, Vec<SampleType>)>> {
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

    let mut sources = HashMap::new();
    for sample in samples.drain(..) {
        sources
            .entry(sample.0)
            .or_insert(Vec::new())
            .push((sample.1, sample.2));
    }
    sources
}

pub fn load_samples(path: &str) -> HashMap<SeriesId, Vec<(u64, Vec<SampleType>)>> {
    let data = load_data(path);
    let data = otlp_data_to_samples(data.as_slice());
    data
}

pub fn mach_register_samples(
    samples: &[(SeriesId, &'static [SampleType])],
    mach: &mut Mach,
    writer: &mut Writer,
) -> Vec<(SeriesRef, &'static [SampleType])> {
    println!("Registering sources to Mach");
    let mut refmap: HashMap<SeriesId, SeriesRef> = HashMap::new();

    let registered_samples: Vec<(SeriesRef, &'static [SampleType])> = samples
        .iter()
        .map(|x| {
            let (id, values) = x;
            let id_ref = *refmap.entry(*id).or_insert_with(|| {
                let conf = get_series_config(*id, &*values);
                let (_, _) = mach.add_series(conf).unwrap();
                let id_ref = writer.get_reference(*id);
                id_ref
            });
            (id_ref, *values)
        })
        .collect();

    registered_samples
}

pub fn get_series_config(id: SeriesId, values: &[SampleType]) -> SeriesConfig {
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

#[derive(Serialize, Deserialize, Clone)]
pub struct ESSample {
    pub series_id: SeriesId,
    timestamp: u64,
    data: Vec<SampleType>,
}

impl ESSample {
    pub fn new(series_id: SeriesId, timestamp: u64, data: Vec<SampleType>) -> Self {
        Self {
            series_id,
            timestamp,
            data,
        }
    }
}

impl From<Sample> for ESSample {
    fn from(data: Sample) -> ESSample {
        ESSample {
            series_id: data.0,
            timestamp: data.1,
            data: data.2,
        }
    }
}

impl Into<JsonBody<serde_json::Value>> for ESSample {
    fn into(self) -> JsonBody<serde_json::Value> {
        serde_json::to_value(self).unwrap().into()
    }
}

impl Into<Vec<u8>> for ESSample {
    fn into(self) -> Vec<u8> {
        serde_json::to_vec(&self).unwrap()
    }
}
