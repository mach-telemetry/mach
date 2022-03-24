# Microbenchmarking harness (thanks Vic!)

## Config

Control things using the config file. Can use the default config, or make a new one

### Different types

Specify the type of the values using valid Rust type in the "value\_type" entry in the config YAML.

Some examples:

```
{ "timestamp": 12345, "value": "some string" } -> value_type: "String"
{ "timestamp": 12345, "value": [1, 2.0] } -> value_type: "[u64, f64]" # fixed length
{ "timestamp": 12345, "value": [1.2, 2.3, 3.4, 4.5, 5.6] } -> value_type: "Vec<usize>" # variable length
```

## Run

```
# Default config file
cargo run --release --bin microbench

# Custom config file
CONFIG=/absolute/path/to/config.yaml cargo run --release --bin microbench
```
