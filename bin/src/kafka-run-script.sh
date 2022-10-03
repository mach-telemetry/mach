#!/bin/bash

KAFKA_BOOTSTRAPS=localhost:9093,localhost:9094,localhost:9095
FILE_PATH=/home/fsolleza/data/intel-telemetry/processed-data.bin
WRITER_BATCHES=500000
DATA_GENERATORS=1
KAFKA_WRITERS=8
KAFKA_PARTITIONS=8

cargo run --release --bin kafka-write-workload -- \
	--kafka-bootstraps $KAFKA_BOOTSTRAPS \
	--file-path $FILE_PATH \
	--writer-batches $WRITER_BATCHES \
	--data-generator-count $DATA_GENERATORS \
	--kafka-writers $KAFKA_WRITERS \
	--kafka-partitions $KAFKA_PARTITIONS




#CARGO_PROFILE_RELEASE_DEBUG=true cargo flamegraph --bin kafka-write-workload -- \
#	--kafka-bootstraps $KAFKA_BOOTSTRAPS \
#	--file-path $FILE_PATH \
#	--writer-batches $WRITER_BATCHES \
#	--data-generator-count $DATA_GENERATORS \
#	--kafka-writers $KAFKA_WRITERS \
#	--kafka-partitions $KAFKA_PARTITIONS

