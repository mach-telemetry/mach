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
    let item_string = std::fs::read_to_string(&conf.item_definition_path).unwrap();
    fs::write(&dest_path, item_string).unwrap();
    println!("cargo:rerun-if-changed=build.rs");
    println!(
        "cargo:rerun-if-changed={}",
        conf.item_definition_path.clone().to_str().unwrap()
    );
}
