#!/bin/bash


FILE_PATH=/home/ubuntu/data/processed-data.bin

OUTPUT=evaluation_output

KAFKA_BOOTSTRAPS=b-3.machkafka.3ebzya.c17.kafka.us-east-1.amazonaws.com:9092,b-2.machkafka.3ebzya.c17.kafka.us-east-1.amazonaws.com:9092,b-1.machkafka.3ebzya.c17.kafka.us-east-1.amazonaws.com:9092


SNAPSHOT_SERVER_PORT=https://172.31.22.116:50051

cargo run --release --bin simple-mach-query -- \
	--query-count 100 \
	--file-path $FILE_PATH \
	--snapshot-server-port $SNAPSHOT_SERVER_PORT \
	> ${OUTPUT}/mach_simple_query_$(date +"%Y%m%d%H%M%S")

#cargo run --release --bin simple-kafka-query -- \
#	--query-count 100 \
#	--file-path $FILE_PATH \
#	--kafka-bootstraps $KAFKA_BOOTSTRAPS \
#	> ${OUTPUT}/kafka_simple_query_$(date +"%Y%m%d%H%M%S")

