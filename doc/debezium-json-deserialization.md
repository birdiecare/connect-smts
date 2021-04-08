# Debezium Json Deserialization

A Postgres' `json` or `jsonb` column doesn't really describe any schema. So when reading such a column,
Debezium converts it in `string`. It means that part of your message is structured, part is nested as a JSON
encoded string. This SMT will automatically decode these JSON strings and transforms them in structured records.

**What are the trade-offs?** The main thing is that you need all values of similar keys to have the exact same type
throughout your records. If not all keys are present all the time, use `optional-struct-fields` to make all fields
optional to prevent backward incompatible changes in the schema that would block your pipeline.

## Usage

```
transforms=json
transforms.json.type=com.birdie.kafka.connect.smt.DebeziumJsonDeserializer
transforms.json.optional-struct-fields=true
```

## Properties

|Name|Description|Type|Default|
|---|---|---|---|
|`optional-struct-fields`| When `true`, all fields in structs are optional. This enables you to have slightly different types within each array item for example, so long that every field with the same name as the same type. | Boolean | `false` |
