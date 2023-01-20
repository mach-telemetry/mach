
### Setup

* install rust
* run the following

```
sudo apt install openssl libssl-dev build-essential pkg-config
cd this/directory
rustup override set nightly
```

### To run a test Kafka setup
```
docker-compose up -d
```

### Directory

* Mach library source code can be found in the `mach` subdirectory
* Example usage and evaluations can be found in the `bin` subdirectory
* Extracted DeathStarBench telemetry data used may be provided upon request

### TODOS

[ ] Need to make readme more understandable

