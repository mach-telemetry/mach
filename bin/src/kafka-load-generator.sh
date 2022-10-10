
WRITER_COUNT=24
BOOTSTRAPS=b-1.machkafka.xoskvb.c17.kafka.us-east-1.amazonaws.com:9092,b-2.machkafka.xoskvb.c17.kafka.us-east-1.amazonaws.com:9092,b-3.machkafka.xoskvb.c17.kafka.us-east-1.amazonaws.com:9092
RATE=500
PARTITIONS=24

cargo run --release --bin kafka-load-generator --\
	--writer-count $WRITER_COUNT \
	--bootstrap-servers $BOOTSTRAPS \
	--rate $RATE \
	--kafka-partitions $PARTITIONS

