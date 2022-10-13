#!/bin/bash


FILE_PATH=/home/ubuntu/data/processed-data.bin
OUTPUT=evaluation_output
QUERY_MIN_DELAY=3
QUERY_MAX_DELAY=10
QUERY_MIN_DURATION=10
QUERY_MAX_DURATION=60

KAFKA_BOOTSTRAPS=b-3.k1.5vjj0v.c17.kafka.us-east-1.amazonaws.com:9092,b-1.k1.5vjj0v.c17.kafka.us-east-1.amazonaws.com:9092,b-2.k1.5vjj0v.c17.kafka.us-east-1.amazonaws.com:9092


SNAPSHOT_SERVER_PORT=https://172.31.22.116:50051

#cargo run --release --bin simple-mach-query -- \
#	--query-count 100 \
#	--file-path $FILE_PATH \
#	--snapshot-server-port $SNAPSHOT_SERVER_PORT \
#	--query-min-delay $QUERY_MIN_DELAY \
#	--query-max-delay $QUERY_MAX_DELAY \
#	--query-min-duration $QUERY_MIN_DURATION \
#	--query-max-duration $QUERY_MAX_DURATION
#	> ${OUTPUT}/mach_simple_query_$(date +"%Y%m%d%H%M%S")

cargo run --release --bin simple-kafka-query -- \
	--query-count 100 \
	--file-path $FILE_PATH \
	--kafka-bootstraps $KAFKA_BOOTSTRAPS \
	--query-min-delay $QUERY_MIN_DELAY \
	--query-max-delay $QUERY_MAX_DELAY \
	--query-min-duration $QUERY_MIN_DURATION \
	--query-max-duration $QUERY_MAX_DURATION
	#> ${OUTPUT}/kafka_simple_query_$(date +"%Y%m%d%H%M%S")

