### Redis Setup

* Run a redis instance

```
docker-compose up -f docker-compose-redis.yaml -d
```

* Check to see if redis is running

```
docker-compose exec redis bash

# Enter redis CLI
redis-cli
```

### Kafka Setup

* Run a single broker kafka

```
docker-compose up -d # might need sudo
```

* Create a topic

```
docker-compose exec broker bash # enters bash in broker container

# Create topic
kafka-topics --create --topic MACHSTORAGE --config="max.message.bytes=10000000" --zookeeper zookeeper:2181 --partitions 1 --replication-factor 1

# Check if topic is created
kafka-topics --list --zookeeper zookeeper:2181

# run tests with kafka
KAFKA=1 cargo test -- --nocapture
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
