mod otlp;

use clap::Parser;
use otlp::OtlpData;

#[derive(Parser, Debug, Clone)]
struct Args {
    #[clap(short, long)]
    input_file_path: String,

    #[clap(short, long)]
    output_file_Path: String,
}


fn main() {
    let args = Args::parse();
    println!("Args: {:#?}", args);

    // Read file
    let mut data = Vec::new();
    File::open(args.input_file_path.as_str()).unwrap().read_to_end(&mut data).unwrap();
    let mut data: Vec<OtlpData> = bincode::deserialize(data.as_slice()).unwrap();
}
