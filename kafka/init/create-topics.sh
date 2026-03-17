#!/bin/bash

echo "Waiting for Kafka..."

sleep 20

kafka-topics --create \
  --if-not-exists \
  --topic "${KAFKA_TOPIC_CRYPTO}" \
  --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS}" \
  --partitions 1 \
  --replication-factor 1
  --add-config retention.ms=86400000,segment.ms=3600000

echo "Kafka topic created: ${KAFKA_TOPIC_CRYPTO}"