package com.birdie.kafka.connect.smt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.*;

public class OutboxTest {
    private Outbox transformer;

    @Before
    public void before(){
        transformer = new Outbox();
        transformer.configure(new HashMap<>() {{
            put("topic", "caregivers.matches.v1");
        }});
    }

    @After
    public void after(){
        transformer.close();
    }

    private final Schema schemaWithPartitionNumber = SchemaBuilder.struct()
            .name("Value")
            .field("key", SchemaBuilder.STRING_SCHEMA)
            .field("partition_number", SchemaBuilder.INT32_SCHEMA)
            .field("payload", SchemaBuilder.string().name("io.debezium.data.Json").optional().build())
            .build();

    private final Schema schemaWithPartitionKey = SchemaBuilder.struct()
            .name("Value")
            .field("key", SchemaBuilder.STRING_SCHEMA)
            .field("partition_key", SchemaBuilder.STRING_SCHEMA)
            .field("payload", SchemaBuilder.string().name("io.debezium.data.Json").optional().build())
            .build();

    private final Schema schemaWithPartitionKeyAndTopic = SchemaBuilder.struct()
            .name("Value")
            .field("key", SchemaBuilder.STRING_SCHEMA)
            .field("partition_key", SchemaBuilder.STRING_SCHEMA)
            .field("topic", SchemaBuilder.STRING_SCHEMA)
            .field("payload", SchemaBuilder.string().name("io.debezium.data.Json").optional().build())
            .build();

    private final Schema schemaWithStringHeaders = SchemaBuilder.struct()
            .name("Value")
            .field("key", SchemaBuilder.STRING_SCHEMA)
            .field("partition_number", SchemaBuilder.INT32_SCHEMA)
            .field("payload", SchemaBuilder.string().name("io.debezium.data.Json").optional().build())
            .field("headers", SchemaBuilder.string().name("io.debezium.data.Json").optional().build())
            .build();

    private final Schema headersStructSchema = SchemaBuilder.struct()
            .name("Headers")
            .field("agency_id", SchemaBuilder.STRING_SCHEMA).optional();

    private final Schema schemaWithStructHeaders = SchemaBuilder.struct()
            .name("Value")
            .field("key", SchemaBuilder.STRING_SCHEMA)
            .field("partition_number", SchemaBuilder.INT32_SCHEMA)
            .field("payload", SchemaBuilder.string().name("io.debezium.data.Json").optional().build())
            .field("headers", headersStructSchema)
            .build();

    @Test
    public void sendsAMessageToCorrectPartitionNumber() {
        Struct value = new Struct(schemaWithPartitionNumber);
        value.put("key", "1234");
        value.put("partition_number", 1);
        value.put("payload", "[\"foo\", \"bar\"]");

        SourceRecord record = new SourceRecord(
                null,
                null,
                "a-database-name.public.the_database_table",
                null,
                SchemaBuilder.bytes().optional().build(),
                "1234".getBytes(),
                schemaWithPartitionNumber,
                value
        );

        final SourceRecord transformedRecord = transformer.apply(record);
        assertEquals("caregivers.matches.v1", transformedRecord.topic());
        assertNotNull(transformedRecord.kafkaPartition());
        assertEquals("1", transformedRecord.kafkaPartition().toString());
        assertEquals("[\"foo\", \"bar\"]", transformedRecord.value());
        assertEquals(Schema.Type.STRING, transformedRecord.valueSchema().type());
    }

    @Test
    public void sendsAMessageWithStructHeaders() {
        Struct headers = new Struct(headersStructSchema);
        headers.put("agency_id", "1234");
        Struct value = new Struct(schemaWithStructHeaders);
        value.put("key", "1234");
        value.put("partition_number", 1);
        value.put("payload", "[\"foo\", \"bar\"]");
        value.put("headers", headers);

        SourceRecord record = new SourceRecord(
                null,
                null,
                "a-database-name.public.the_database_table",
                null,
                SchemaBuilder.bytes().optional().build(),
                "1234".getBytes(),
                schemaWithStructHeaders,
                value
        );

        final SourceRecord transformedRecord = transformer.apply(record);
        assertEquals("1234", transformedRecord.headers().lastWithName("agency_id").value());
    }

    @Test
    public void sendsAMessageWithStringHeaders() {
        Struct value = new Struct(schemaWithStringHeaders);
        value.put("key", "1234");
        value.put("partition_number", 1);
        value.put("payload", "[\"foo\", \"bar\"]");
        value.put("headers", "{\"agency_id\": \"1234\"}");

        SourceRecord record = new SourceRecord(
                null,
                null,
                "a-database-name.public.the_database_table",
                null,
                SchemaBuilder.bytes().optional().build(),
                "1234".getBytes(),
                schemaWithStringHeaders,
                value
        );

        final SourceRecord transformedRecord = transformer.apply(record);
        assertEquals("1234", transformedRecord.headers().lastWithName("agency_id").value());
    }

    @Test
    public void sendsAMessageWithHeadersContainingEmptyAndNumericValues() {
        Struct value = new Struct(schemaWithStringHeaders);
        value.put("key", "1234");
        value.put("partition_number", 1);
        value.put("payload", "[\"foo\", \"bar\"]");
        value.put("headers", "{\"event_number\": 1234, \"agency_id\": null}");

        SourceRecord record = new SourceRecord(
                null,
                null,
                "a-database-name.public.the_database_table",
                null,
                SchemaBuilder.bytes().optional().build(),
                "1234".getBytes(),
                schemaWithStringHeaders,
                value
        );

        final SourceRecord transformedRecord = transformer.apply(record);
        assertEquals("1234", transformedRecord.headers().lastWithName("event_number").value());
        assertNull(transformedRecord.headers().lastWithName("agency_id").value());
    }

    @Test
    public void sendsAMessageWithNullHeaders() {
        Struct value = new Struct(schemaWithStringHeaders);
        value.put("key", "1234");
        value.put("partition_number", 1);
        value.put("payload", "[\"foo\", \"bar\"]");
        value.put("headers", null);

        SourceRecord record = new SourceRecord(
                null,
                null,
                "a-database-name.public.the_database_table",
                null,
                SchemaBuilder.bytes().optional().build(),
                "1234".getBytes(),
                schemaWithStringHeaders,
                value
        );

        final SourceRecord transformedRecord = transformer.apply(record);
        assertEquals("caregivers.matches.v1", transformedRecord.topic());
        assertEquals("[\"foo\", \"bar\"]", transformedRecord.value());
        assertTrue(transformedRecord.headers().isEmpty());
    }

    @Test
    public void sendsMessagesUsingPartitionKey() {
        transformer.configure(new HashMap<>() {{
            put("topic", "caregivers.matches.v1");
            put("partition-setting", "partition-key");
            put("num-partitions", 3);
        }});

        Struct value1 = new Struct(schemaWithPartitionKey);
        value1.put("key", "1234");
        value1.put("partition_key", "some-partition-key");
        value1.put("payload", "[\"foo\", \"bar\"]");

        Struct value2 = new Struct(schemaWithPartitionKey);
        value2.put("key", "1234");
        value2.put("partition_key", "another-partition-key");
        value2.put("payload", "[\"foo\", \"bar\"]");

        SourceRecord record1 = new SourceRecord(
                null,
                null,
                "a-database-name.public.the_database_table",
                null,
                SchemaBuilder.bytes().optional().build(),
                "1234".getBytes(),
                schemaWithPartitionKey,
                value1
        );

        SourceRecord record2 = new SourceRecord(
                null,
                null,
                "a-database-name.public.the_database_table",
                null,
                SchemaBuilder.bytes().optional().build(),
                "1234".getBytes(),
                schemaWithPartitionKey,
                value2
        );

        final SourceRecord transformedRecord1 = transformer.apply(record1);
        final SourceRecord transformedRecord2 = transformer.apply(record2);
        assertEquals("1", transformedRecord1.kafkaPartition().toString());
        assertEquals("some-partition-key", transformedRecord1.headers().lastWithName("partition_key").value());
        assertEquals("2", transformedRecord2.kafkaPartition().toString());
        assertEquals("another-partition-key", transformedRecord2.headers().lastWithName("partition_key").value());
    }

    @Test
    public void throwsIfUsingPartitionKeySettingWithoutPartitionKey() {
        transformer.configure(new HashMap<>() {{
            put("topic", "caregivers.matches.v1");
            put("partition-setting", "partition-key");
            put("num-partitions", 3);
        }});

        Struct value = new Struct(schemaWithPartitionKey);
        value.put("key", "1234");
        value.put("payload", "[\"foo\", \"bar\"]");

        SourceRecord record = new SourceRecord(
                null,
                null,
                "a-database-name.public.the_database_table",
                null,
                SchemaBuilder.bytes().optional().build(),
                "1234".getBytes(),
                schemaWithPartitionKey,
                value
        );

        Exception error = assertThrows(Exception.class, () -> {
            transformer.apply(record);
        });
        assertTrue(error.getMessage().contains("Unable to find partition_key in source record"));
    }

    @Test
    public void throwsIfUsingPartitionNumberSettingWithoutPartitionNumber() {
        transformer.configure(new HashMap<>() {{
            put("topic", "caregivers.matches.v1");
            put("partition-setting", "partition-number");
        }});

        Struct value = new Struct(schemaWithPartitionKey);
        value.put("key", "1234");
        value.put("payload", "[\"foo\", \"bar\"]");

        SourceRecord record = new SourceRecord(
                null,
                null,
                "a-database-name.public.the_database_table",
                null,
                SchemaBuilder.bytes().optional().build(),
                "1234".getBytes(),
                schemaWithPartitionKey,
                value
        );

        Exception error = assertThrows(Exception.class, () -> {
            transformer.apply(record);
        });
        assertTrue(error.getMessage().contains("Unable to find partition_number in source record"));
    }

    @Test
    public void ignoresDebeziumTombstones() {
        SourceRecord tombstone = new SourceRecord(
            null,
            null,
            "a-database-name.public.the_database_table",
            null,
            SchemaBuilder.bytes().optional().build(),
            "1234".getBytes(),
            null,
            null // null value means we don't know what is the correct partition
        );
        assertNull(transformer.apply(tombstone));
    }

    @Test
    public void createsTombstoneOutOfDebeziumDeleteRecord() {
        Schema deleteSchema = SchemaBuilder.struct()
            .name("Value")
            .field("key", SchemaBuilder.STRING_SCHEMA)
            .field("partition_number", SchemaBuilder.INT32_SCHEMA)
            .field("__deleted", SchemaBuilder.STRING_SCHEMA)
            .build();
        Struct value = new Struct(deleteSchema);
        value.put("key", "1234");
        value.put("partition_number", 1);
        value.put("__deleted", "true");

        SourceRecord deleteRecord = new SourceRecord(
            null,
            null,
            "a-database-name.public.the_database_table",
            null,
            SchemaBuilder.bytes().optional().build(),
            "1234".getBytes(),
            deleteSchema,
            value
        );

        final SourceRecord transformedRecord = transformer.apply(deleteRecord);
        assertEquals("caregivers.matches.v1", transformedRecord.topic());
        assertEquals("1", transformedRecord.kafkaPartition().toString());
        assertNull(transformedRecord.valueSchema());
        assertNull(transformedRecord.value());
    }

    @Test
    public void sendsAMessageToTheTopicRequestedInTheTable() {
        Struct value = new Struct(schemaWithPartitionKeyAndTopic);
        value.put("key", "1234");
        value.put("partition_key", "1234-5678");
        value.put("topic", "my.topic.v1");
        value.put("payload", "[\"foo\", \"bar\"]");

        // Re-configure the outbox without a topic
        transformer = new Outbox();
        transformer.configure(new HashMap<>() {{
            put("partition-setting", "partition-key");
            put("num-partitions", 3);
        }});

        SourceRecord record = new SourceRecord(
                null,
                null,
                "a-database-name.public.the_database_table",
                null,
                SchemaBuilder.bytes().optional().build(),
                "1234".getBytes(),
                schemaWithPartitionKeyAndTopic,
                value
        );

        final SourceRecord transformedRecord = transformer.apply(record);
        assertEquals("my.topic.v1", transformedRecord.topic());
    }

    @Test
    public void sendsAMessageToTheTopicRequestedInTheTableWithoutSMTPartitionConfiguration() {
        Struct value = new Struct(schemaWithPartitionKeyAndTopic);
        value.put("key", "1234");
        value.put("partition_key", "1234-5678");
        value.put("topic", "my.topic.v1@3");
        value.put("payload", "[\"foo\", \"bar\"]");

        // Re-configure the outbox without a topic
        transformer = new Outbox();
        transformer.configure(new HashMap<>() {{
            put("partition-setting", "partition-key");
        }});

        SourceRecord record = new SourceRecord(
                null,
                null,
                "a-database-name.public.the_database_table",
                null,
                SchemaBuilder.bytes().optional().build(),
                "1234".getBytes(),
                schemaWithPartitionKeyAndTopic,
                value
        );

        final SourceRecord transformedRecord = transformer.apply(record);
        assertEquals("my.topic.v1", transformedRecord.topic());
        assertEquals("2", transformedRecord.kafkaPartition().toString());
    }

    @Test
    public void sendsATombstoneWhenThePayloadIsNull() {
        Struct value = new Struct(schemaWithPartitionKeyAndTopic);
        value.put("key", "1234");
        value.put("partition_key", "1234-5678");
        value.put("topic", "my.topic.v1");
        value.put("payload", null);

        // Re-configure the outbox without a topic
        transformer = new Outbox();
        transformer.configure(new HashMap<>() {{
            put("partition-setting", "partition-key");
            put("num-partitions", 3);
        }});

        SourceRecord record = new SourceRecord(
                null,
                null,
                "a-database-name.public.the_database_table",
                null,
                SchemaBuilder.bytes().optional().build(),
                "1234".getBytes(),
                schemaWithPartitionKeyAndTopic,
                value
        );

        final SourceRecord transformedRecord = transformer.apply(record);
        assertEquals("my.topic.v1", transformedRecord.topic());

        assertNull(transformedRecord.value());
        assertNull(transformedRecord.valueSchema());
    }
}
