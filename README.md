# Birdie's Kafka Connect SMTs

This repository contains Kafka Connect SMTs used by Birdie.

1. [JOSE encryption](#jose-encryption) to (de)crypt messages using JOSE.
2. [Nested JSON schema](#nested-json-schema) to create schema for JSON columns fetched by Debezium.

# JOSE encryption

## Usage

```
value.converter=org.apache.kafka.connect.converters.ByteArrayConverter

transforms=jose
transforms.jose.type=com.birdie.kafka.connect.smt.Jose$DecryptValue
transforms.jose.key="encryption-key"
```

## Properties

|Name|Description|Type|Default|
|---|---|---|---|---|
|`keys`| Comma separated list of base64 encoded JWKs | String | ø |
|`skip-on-error`| If `true`, skips messages it cannot decrypt (i.e. keeps the original payload) | String | `false` |

# Nested JSON schema

Debezium doesn't create a schema for JSON column, mostly because this schema doesn't exist in the databases. This is
[not going to be supported in Debezium core any time soon.](https://issues.redhat.com/browse/DBZ-3104) because of the
trade-offs that needs to be made if the structure changes too often over time.

## Usage

```
transforms=json
transforms.json.type=com.birdie.kafka.connect.smt.JsonSchema
transforms.json.no-schema-behaviour=fail
transforms.json.columns.tasks.schema=https://visit-planning.production.birdie.care/docs.json#VisitInstance.tasks
```

## Properties

None.

# Development

```
mvn test
```
