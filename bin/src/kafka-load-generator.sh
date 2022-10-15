WRITER_COUNT=64
BOOTSTRAPS=b-2.francocluster.fxyjf4.c17.kafka.us-east-1.amazonaws.com:9092,b-3.francocluster.fxyjf4.c17.kafka.us-east-1.amazonaws.com:9092,b-1.francocluster.fxyjf4.c17.kafka.us-east-1.amazonaws.com:9092
RATE=300
PARTITIONS=24

cargo run --release --bin kafka-load-generator --\
	--writer-count $WRITER_COUNT \
	--bootstrap-servers $BOOTSTRAPS \
	--rate $RATE \
	--kafka-partitions $PARTITIONS

