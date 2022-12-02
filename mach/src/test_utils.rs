use crate::active_segment::{ActiveSegmentWriter, PushStatus};
use crate::field_type::*;
use crate::sample::SampleType;
use crate::utils;
use rand::{
    distributions::{Alphanumeric, DistString},
    thread_rng, Rng,
};
use std::ops::Deref;
use std::ops::Range;
use std::sync::Arc;

pub struct Samples {
    data: Arc<Vec<Vec<SampleType>>>,
}

impl Deref for Samples {
    type Target = [Vec<SampleType>];
    fn deref(&self) -> &Self::Target {
        self.data.as_slice()
    }
}

pub fn random_samples(types: &[FieldType], n_samples: usize, str_sz: Range<usize>) -> Samples {
    let mut rng = thread_rng();
    let mut v = Vec::new();
    for field_type in types {
        match field_type {
            FieldType::F64 => {
                let expected_floats: Vec<SampleType> =
                    (0..n_samples).map(|_| SampleType::F64(rng.gen())).collect();
                v.push(expected_floats);
            }
            FieldType::Bytes => {
                let expected_strings: Vec<SampleType> = (0..n_samples)
                    .map(|_| {
                        let str_len = rng.gen_range(str_sz.clone());
                        let string = Alphanumeric.sample_string(&mut rng, str_len);
                        SampleType::Bytes(string.into_bytes())
                    })
                    .collect();
                v.push(expected_strings);
            }
            _ => unimplemented!(),
        }
    }

    Samples { data: Arc::new(v) }
}

pub fn fill_active_segment(samples: &Samples, w: &mut ActiveSegmentWriter) -> usize {
    let mut values = Vec::new();
    let mut counter = 0;
    loop {
        let i = counter;
        for col in samples.iter() {
            values.push(col[i].clone());
        }
        counter += 1;
        match w.push(utils::now_in_micros(), values.as_slice()) {
            PushStatus::Ok => {}
            PushStatus::Full => break,
            PushStatus::ErrorFull => unreachable!(),
        }
        values.clear();
    }
    counter
}
