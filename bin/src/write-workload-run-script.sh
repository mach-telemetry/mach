#!/bin/bash


FILE_PATH=/home/ubuntu/data/processed-data.bin
#FILE_PATH=/home/fsolleza/data/intel-telemetry/processed-data.bin

OUTPUT=evaluation_output

WRITER_BATCHES=500000
DATA_GENERATORS=1
WRITER_COUNT=1
SOURCE_COUNT=1000
UNBOUNDED_QUEUE=--unbounded-queue

# - KAFKA parameters
KAFKA_PARTITIONS=24
KAFKA_BATCH_BYTES=1000000
KAFKA_BOOTSTRAPS=localhost:9093,localhost:9094,localhost:9095
KAFKA_BOOTSTRAPS=b-3.k1.a76n55.c17.kafka.us-east-1.amazonaws.com:9092,b-2.k1.a76n55.c17.kafka.us-east-1.amazonaws.com:9092,b-1.k1.a76n55.c17.kafka.us-east-1.amazonaws.com:9092

QUERIER_IP=172.31.78.194

KAFKA_OUT_FILE=${OUTPUT}/kafka_ingest_${WRITER_COUNT}_writers_${WRITER_BATCHES}_batch_${SOURCE_COUNT}_sources_$(date +"%Y%m%d%H%M%S")

MACH_OUT_FILE=${OUTPUT}/mach_ingest_${WRITER_COUNT}_writers_${WRITER_BATCHES}_batch_${SOURCE_COUNT}_sources_$(date +"%Y%m%d%H%M%S")

cargo run --release --bin kafka-write-workload -- \
	--kafka-bootstraps $KAFKA_BOOTSTRAPS \
	--file-path $FILE_PATH \
	--writer-batches $WRITER_BATCHES \
	--data-generator-count $DATA_GENERATORS \
	--kafka-writers $WRITER_COUNT \
	--kafka-partitions $KAFKA_PARTITIONS \
	--kafka-batch-bytes $KAFKA_BATCH_BYTES \
	--querier-ip $QUERIER_IP \
	${UNBOUNDED_QUEUE} \
	> $KAFKA_OUT_FILE


#cargo run --release --bin mach-write-workload -- \
#	--file-path $FILE_PATH \
#	--writer-batches $WRITER_BATCHES \
#	--data-generator-count $DATA_GENERATORS \
#	--mach-writers $WRITER_COUNT \
#	--source-count $SOURCE_COUNT \
#	--querier-ip $QUERIER_IP \
#	${UNBOUNDED_QUEUE} \
#	> $MACH_OUT_FILE
