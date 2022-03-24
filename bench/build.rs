use std::env;
use std::fs;
use std::path::Path;

mod config {
    include!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/config.rs"));
}

fn main() {
    let conf: config::Config = config::load_conf();
    let out_dir = env::var_os("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("item.rs");
    let mut item_string = format!("
use serde::*;
#[derive(Serialize, Deserialize)]
struct Item {{
    timestamp: u64,
    value: {},
}}

impl Item {{
    fn from_str(s: &String) -> Item {{
        let item: Item = serde_json::from_str(s).expect(\"cannot parse data item\");
        item
    }}
}}
    ", conf.value_type);
    fs::write(
        &dest_path,
        item_string
    ).unwrap();
    println!("cargo:rerun-if-changed=build.rs");
}
