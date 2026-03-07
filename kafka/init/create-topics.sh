#!/bin/bash

echo "Waiting for Kafka..."

sleep 20

kafka-topics --create \
  --if-not-exists \
  --topic job_postings_raw \
  --bootstrap-server kafka:9092 \
  --partitions 1 \
  --replication-factor 1

echo "Kafka topic created"