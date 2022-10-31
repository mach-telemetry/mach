use crate::field_type::*;
use crate::sample::SampleType;
use rand::{
    distributions::{Alphanumeric, DistString},
    thread_rng, Rng,
};

pub fn random_samples(types: &[FieldType], n_samples: usize) -> Vec<Vec<SampleType>> {
    let mut rng = thread_rng();
    let mut v = Vec::new();
    for field_type in types {
        match field_type {
            FieldType::F64 => {
                let expected_floats: Vec<SampleType> =
                    (0..n_samples).map(|_| SampleType::F64(rng.gen())).collect();
                v.push(expected_floats);
            },
            FieldType::Bytes => {
                let expected_strings: Vec<SampleType> = (0..n_samples)
                    .map(|_| {
                        let string = Alphanumeric.sample_string(&mut rng, 16);
                        SampleType::Bytes(string.into_bytes())
                    })
                    .collect();
                v.push(expected_strings);
            }
            _ => unimplemented!(),
        }
    }
    v
}

