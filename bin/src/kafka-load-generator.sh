WRITER_COUNT=128
BOOTSTRAPS=b-1.francocluster2.wseolp.c17.kafka.us-east-1.amazonaws.com:9092,b-2.francocluster2.wseolp.c17.kafka.us-east-1.amazonaws.com:9092,b-3.francocluster2.wseolp.c17.kafka.us-east-1.amazonaws.com:9092
RATE=1000
PARTITIONS=24
TOPICS=16

cargo run --release --bin kafka-load-generator --\
	--writer-count $WRITER_COUNT \
	--bootstrap-servers $BOOTSTRAPS \
	--rate $RATE \
	--kafka-partitions $PARTITIONS \
	--kafka-topics $TOPICS

