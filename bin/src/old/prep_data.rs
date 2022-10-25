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
use std::collections::HashMap;

pub type TimeStamp = u64;
pub type Sample = (SeriesId, TimeStamp, Vec<SampleType>);
pub type RegisteredSample = (SeriesRef, TimeStamp, Vec<SampleType>);

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

#[derive(Serialize)]
pub struct ESSampleRef<'a> {
    series_id: u64,
    timestamp: u64,
    data: &'a [SampleType],
}

impl<'a> From<&'a (u64, u64, Vec<SampleType>)> for ESSampleRef<'a> {
    fn from(other: &'a (u64, u64, Vec<SampleType>)) -> Self {
        Self {
            series_id: other.0,
            timestamp: other.1,
            data: other.2.as_slice(),
        }
    }
}

impl Into<Vec<u8>> for ESSampleRef<'_> {
    fn into(self) -> Vec<u8> {
        serde_json::to_vec(&self).unwrap()
    }
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