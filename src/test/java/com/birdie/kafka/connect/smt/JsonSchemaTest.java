package com.birdie.kafka.connect.smt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class JsonSchemaTest {
    private JsonSchema transformer = new JsonSchema();

    @After
    public void tearDown() throws Exception {
        transformer.close();
    }

    @Test
    public void doSomething() {
        final Map<String, String> props = new HashMap<String, String>() {{
            put("no-schema-behaviour", "fail");
            put("columns.tasks.schema", "https://visit-planning.production.birdie.care/docs.json#VisitInstance.tasks");
        }};

        transformer.configure(props);

        Schema valueSchema = SchemaBuilder.struct()
                .field("id", SchemaBuilder.STRING_SCHEMA)
                .field("tasks", SchemaBuilder.string().name("io.debezium.data.Json").build())
                .build();

        Struct value = new Struct(valueSchema);
        value.put("id", "1234-5678");
        value.put("tasks", "[\"foo\", \"bar\"]");

        final SourceRecord record = new SourceRecord(
                null, null, "test", 0,
                SchemaBuilder.bytes().optional().build(), "key".getBytes(), valueSchema, value);

        final SourceRecord transformedRecord = transformer.apply(record);

        assertNotNull(transformedRecord.valueSchema());
//        assertEquals(Schema.Type.ARRAY, transformedRecord.valueSchema().field("tasks").schema().type());
//        assertEquals(Schema.Type.STRING, transformedRecord.valueSchema().field("tasks").schema().schema().type());
    }
}
