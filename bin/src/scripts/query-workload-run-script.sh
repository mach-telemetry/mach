#!/bin/bash

ES_INDEX=es06

#FILE_PATH=/home/ubuntu/data/processed-data.bin
FILE_PATH=/home/fsolleza/data/intel-telemetry/processed-data.bin
OUTDIR=$(dirname $0)/output
QUERY_MIN_DELAY=3
QUERY_MAX_DELAY=10
QUERY_MIN_DURATION=10
QUERY_MAX_DURATION=60
KAFKA_BOOTSTRAPS=localhost:9093,localhost:9094,localhost:9095

SNAPSHOT_SERVER_PORT=https://10.116.60.21:50051
MACH_OUTPUT=${OUTDIR}/mach_simple_query_$(date +"%Y%m%d%H%M%S")
MACH_OUTPUT=${OUTDIR}/mach_query_tmp

cargo run --release --bin simple-mach-query -- \
	--query-count 150 \
	--file-path $FILE_PATH \
	--snapshot-server-port $SNAPSHOT_SERVER_PORT \
	--query-min-delay $QUERY_MIN_DELAY \
	--query-max-delay $QUERY_MAX_DELAY \
	--query-min-duration $QUERY_MIN_DURATION \
	--query-max-duration $QUERY_MAX_DURATION \
	> $MACH_OUTPUT

KAFKA_OUTPUT=${OUTDIR}/kafka_simple_query_$(date +"%Y%m%d%H%M%S")
KAFKA_OUTPUT=${OUTDIR}/kafka_query_tmp

#cargo run --release --bin simple-kafka-query -- \
#	--query-count 100 \
#	--file-path $FILE_PATH \
#	--kafka-bootstraps $KAFKA_BOOTSTRAPS \
#	--query-min-delay $QUERY_MIN_DELAY \
#	--query-max-delay $QUERY_MAX_DELAY \
#	--query-min-duration $QUERY_MIN_DURATION \
#	--query-max-duration $QUERY_MAX_DURATION \
#	> $KAFKA_OUTPUT

ES_ENDPOINT=https://vpc-es01-6wvpspb2kiqdbokovy7svaxqli.us-east-1.es.amazonaws.com

#cargo run --release --bin simple-es-query -- \
#    --query-count 1000 \
#	--query-min-delay $QUERY_MIN_DELAY \
#	--query-max-delay $QUERY_MAX_DELAY \
#	--query-min-duration $QUERY_MIN_DURATION \
#	--query-max-duration $QUERY_MAX_DURATION \
#    --file-path $FILE_PATH \
#    --es-endpoint $ES_ENDPOINT \
#    --es-index-name $ES_INDEX \
#    > ${OUTPUT}/es_query_out
