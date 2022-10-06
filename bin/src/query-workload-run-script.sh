#!/bin/bash


FILE_PATH=/home/ubuntu/data/processed-data.bin

OUTPUT=evaluation_output

KAFKA_BOOTSTRAPS=b-1.machkafka.6qa2b7.c17.kafka.us-east-1.amazonaws.com:9092,b-2.machkafka.6qa2b7.c17.kafka.us-east-1.amazonaws.com:9092,b-3.machkafka.6qa2b7.c17.kafka.us-east-1.amazonaws.com:9092


SNAPSHOT_SERVER_PORT=https://172.31.22.116:50051
cargo run --release --bin simple-mach-query -- \
	--query-count 100 \
	--file-path $FILE_PATH \
	--snapshot-server-port $SNAPSHOT_SERVER_PORT \
	> ${OUTPUT}/mach_simple_query_$(date +"%Y%m%d%H%M%S")

