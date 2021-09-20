# Outbox

The [outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html) enables to reliably send messages
to a message bus / event streaming platform. 

The usual outbox SMT ([like Debezium's](https://debezium.io/documentation/reference/transformations/outbox-event-router.html))
send all events of the same "aggregate" with the same key, so that multiple events related to the same user for instance
end up in the same partition. However, it means that we can't enable compaction, meaning that we can't support deletion
of events through tomstones.

This SMT will take care of the partition routing (from a `partition_key`) while keeping the event identifier as the Kafka
message key. You can therefore maintain ordering for an aggregate while removing individual events through compaction.

## Usage

The transformed message is expected to have the following fields (if you use Debezium, the table is expected to contain 
the following columns and [to have been flattened](https://debezium.io/documentation/reference/transformations/event-flattening.html)):
- `payload`
- `partition_number` or `partition_key`
- `headers` (optional)

```
transforms=outbox
transforms.outbox.type=com.birdie.kafka.connect.smt.Outbox
transforms.outbox.topic="target-topic"
```

## Properties

|Name|Description|Type|Default|
|---|---|---|---|
|`topic`| The name of the topic to send messages to | String | ø |
|`partition-setting`| If `partition-key`, will decide the partition from the `partition_key`, if `partition-number`, will pick directly from the `partition_number` column | String | `partition-number` |
|`num-partitions`| The number of partitions in your target topic | String | `false` |
| `debug-log-level` | The log level setting (trace/debug/info) for debugging logs, to track exactly what record was received and what was transformed/sent | String | `trace`
