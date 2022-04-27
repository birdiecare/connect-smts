# Birdie's Kafka Connect SMTs

This repository contains Kafka Connect SMTs used by Birdie.

1. [Debezium JSON deserialization](./doc/debezium-json-deserialization.md) to create schema for JSON columns fetched by Debezium.
2. [JOSE decryption](./doc/jose-decryption.md) to decrypt messages using JOSE.
3. [Outbox](./doc/outbox.md) Outbox pattert with partition routing.


# Development

```
mvn test
```
