### Kafka Setup

* Run a single broker kafka

```
docker-compose up -d # might need sudo
```

* Check if reachable

```
docker-compose exec broker bash # enters bash in broker container

# Create topic
kafka-topics --create --topic testing --config="max.message.bytes=10000000" --zookeeper zookeeper:2181 --partitions 1 --replication-factor 1

# Check if topic is created
kafka-topics --list --zookeeper zookeeper:2181

# run tests with kafka
cargo test --features kafka-backend
```

### Setup

* install rust
* run the following

```
cd this/directory
rustup override set nightly
tar -xzf data.tar.gz
cargo test
```

### Data
`tar -xzf data.tar.gz` extracts test univariate and multivariate data into `mach-private/data`.
This directory is ignored in `.gitignore`

### MOAR data

`wget https://www.dropbox.com/s/ya7mwmvq17v6hqh/data.tar.gz`
