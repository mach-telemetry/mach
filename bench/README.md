# Microbenchmarking harness (thanks Vic!)

## Config

Control things using the config file. Can use the default config, or make a new one

## Run

```
# Default config file
cargo run --release --bin microbench

# Custom config file
CONFIG=/absolute/path/to/config.yaml cargo run --release --bin microbench
```

### Different types in the data

Specify the expected type using the `item.rs` file as a template. The new file should be reflected
in the `config.yaml` in `item_definition_path`
