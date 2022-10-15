#!/bin/bash

KAFKA_BOOTSTRAPS=b-3.k2.ini5pv.c17.kafka.us-east-1.amazonaws.com:9092,b-1.k2.ini5pv.c17.kafka.us-east-1.amazonaws.com:9092,b-2.k2.ini5pv.c17.kafka.us-east-1.amazonaws.com:9092
ES_ENDPOINT=https://vpc-es01-6wvpspb2kiqdbokovy7svaxqli.us-east-1.es.amazonaws.com
ES_INDEX=es06

KAFKA_PARTITIONS=24
KAFKA_TOPIC=k1

UNBOUNDED_QUEUE=--unbounded-queue

OUTPUT=evaluation_output

cargo run --release --bin kafka-es-connector -- \
   --kafka-topic $KAFKA_TOPIC \
   --kafka-bootstraps $KAFKA_BOOTSTRAPS \
   --kafka-partitions $KAFKA_PARTITIONS \
   --es-endpoint $ES_ENDPOINT \
   --es-index-name $ES_INDEX \
   ${UNBOUNDED_QUEUE} \
   > ${OUTPUT}/kafka_es_connector_out
