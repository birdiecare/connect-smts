package com.birdie.kafka.connect.smt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class OutboxTest {
    @Test
    public void sendsAMessageToTheCorrectTopicPartition() {
        Outbox transformer = new Outbox();
        transformer.configure(new HashMap<>() {{
            put("topic", "caregivers.matches.v1");
        }});

        Schema schema = SchemaBuilder.struct()
                .name("Value")
                .field("key", SchemaBuilder.STRING_SCHEMA)
                // Later: `partition_key`, SMT will figure out the number of partition of the target topic by itself
                .field("partition_number", SchemaBuilder.INT32_SCHEMA)
                .field("payload", SchemaBuilder.string().name("io.debezium.data.Json").optional().build())
            .build();

        Struct value = new Struct(schema);
        value.put("key", "1234");
        value.put("partition_number", 1);
        value.put("payload", "[\"foo\", \"bar\"]");

        SourceRecord record = new SourceRecord(
                // Where it comes from
                null, null,
                // Where it is going
                "a-database-name.public.the_database_table", null,
                // Key
                SchemaBuilder.bytes().optional().build(),
                "1234".getBytes(),
                // Value
                schema,
                value
        );

        final SourceRecord transformedRecord = transformer.apply(record);

        assertEquals("caregivers.matches.v1", transformedRecord.topic());
        assertNotNull(transformedRecord.kafkaPartition());
        assertEquals("1", transformedRecord.kafkaPartition().toString());
    }
}
