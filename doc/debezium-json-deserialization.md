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
|`union-previous-messages-schema`| When `true`, the schema will be merged with previous messages' schemas. If you have lots of different schema structures in your table, this will help reduce the number of schema versions being created. | Boolean | `false` |
|`union-previous-messages-schema.log-union-errors`| When `true`, if two schemas can't be merged with one another, it will log an error instead of just considering it normal. | Boolean | `false` |
|`union-previous-messages-schema.topic.{topic-name}.field.{field-name}`| An array of Kafka Connect schema to be used as initial schema(s) to unify messages with. It's an array in case some are incompatible, on the same field. You can get the serialized schema from the SMT logs as it processes messages. | String | ø |
|`convert-numbers-to-double`| When `true`, all number fields in structs are converted to double. This avoids compatibility errors when some fields can contain both integers and floats. | Boolean | `false` |
|`sanitize.field.names`| When `true`, sanitizes the fields name so they are compatible with Avro; [like with Debezium.](https://debezium.io/documentation/reference/1.4/configuration/avro.html#avro-naming) | Boolean | `false` |

## Benchmark

The latest benchmarks (MacBook Pro; 13-inch, 2019) shows the following:
 
- Without `union-previous-messages-schema`, ~4700 ops/sec.
- With `union-previous-messages-schema`, ~3500 ops/sec.
