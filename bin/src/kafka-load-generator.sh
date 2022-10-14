WRITER_COUNT=150
BOOTSTRAPS=b-3.k1.ge0foy.c17.kafka.us-east-1.amazonaws.com:9092,b-1.k1.ge0foy.c17.kafka.us-east-1.amazonaws.com:9092,b-2.k1.ge0foy.c17.kafka.us-east-1.amazonaws.com:9092
RATE=300
PARTITIONS=24

cargo run --release --bin kafka-load-generator --\
	--writer-count $WRITER_COUNT \
	--bootstrap-servers $BOOTSTRAPS \
	--rate $RATE \
	--kafka-partitions $PARTITIONS

