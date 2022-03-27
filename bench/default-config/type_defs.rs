// NOTE: Don't change anything other than the specified areas

use serde::*;

fn schema() -> (Vec<Types>, Vec<CompressFn>) {
    /// Change to reflect the series schema
    (vec![Types::Bytes], vec![CompressFn::BytesLZ4])
}

#[derive(Serialize, Deserialize)]
struct Item {
    timestamp: u64,

    /// Change this to reflect a different type in the JSON. For example:
    /// { "timestamp": 12345, "value": "some string" } -> value_type: "String"
    /// { "timestamp": 12345, "value": [1, 2.0] } -> value_type: "[u64, f64]" # fixed length
    /// { "timestamp": 12345, "value": [1.2, 2.3, 3.4, 4.5, 5.6] } -> value_type: "Vec<usize>" # variable length
    value: String,
}

impl Item {
    fn from_str(s: &String) -> Item {
        let item: Item = serde_json::from_str(s).expect("cannot parse data item");
        item
    }

    fn value_types(&self) -> Vec<Type> {
        /// Write logic here to handle conversion from self.value to Vec<Type>
        vec![Type::Bytes(Bytes::from_slice(self.value.as_bytes()))]
    }
}

