#!/bin/bash

#FILE_PATH=/home/fsolleza/data/intel-telemetry/processed-data.bin

OUTPUT=evaluation_output

FILE_PATH=/home/ubuntu/data/processed-data.bin
WRITER_BATCHES=500000
DATA_GENERATORS=1
WRITER_COUNT=1

# - KAFKA parameters
KAFKA_PARTITIONS=24
KAFKA_BATCH_BYTES=1000000
KAFKA_BOOTSTRAPS=localhost:9093,localhost:9094,localhost:9095
KAFKA_BOOTSTRAPS=b-2.machkafka1.kgw98m.c25.kafka.us-east-1.amazonaws.com:9092,b-3.machkafka1.kgw98m.c25.kafka.us-east-1.amazonaws.com:9092,b-1.machkafka1.kgw98m.c25.kafka.us-east-1.amazonaws.com:9092

cargo run --release --bin kafka-write-workload -- \
	--kafka-bootstraps $KAFKA_BOOTSTRAPS \
	--file-path $FILE_PATH \
	--writer-batches $WRITER_BATCHES \
	--data-generator-count $DATA_GENERATORS \
	--kafka-writers $WRITER_COUNT \
	--kafka-partitions $KAFKA_PARTITIONS \
	--kafka-batch-bytes $KAFKA_BATCH_BYTES \
	> ${OUTPUT}/kafka_ingest_${WRITER_COUNT}_writers_${WRITER_BATCHES}_batch_2


#cargo run --release --bin mach-write-workload -- \
#	--file-path $FILE_PATH \
#	--writer-batches $WRITER_BATCHES \
#	--data-generator-count $DATA_GENERATORS \
#	--mach-writers $WRITER_COUNT \
#	> ${OUTPUT}/mach_ingest_${WRITER_COUNT}_writers_${WRITER_BATCHES}_batch

