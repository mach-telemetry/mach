mod prep_data;
mod bytes_server;
mod snapshotter;

use regex::Regex;

fn main() {
    let data: Vec<prep_data::Sample> = prep_data::load_samples("/home/sli/data/train-ticket-data");
    let re = Regex::new(r"Error").unwrap();
    for sample in data.iter() {
        let span: otlp::trace::v1::Span = bincode::deserialize(&sample.2[0].as_bytes()).unwrap();
        for kv in span.events[0].attributes.iter() {
            let value = kv.value.as_ref().unwrap().as_str();
            println!("{:?} {}", value, re.find(value).is_some());
        }
        break;
    }
}
