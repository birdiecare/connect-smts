package com.birdie.kafka.connect.smt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class DebeziumJsonDeserializerTest {
    private static final Schema simpleSchema = SchemaBuilder.struct()
        .name("Value")
        .field("id", SchemaBuilder.STRING_SCHEMA)
        .field("json", SchemaBuilder.string().name("io.debezium.data.Json").optional().build())
        .build();

    private SourceRecord doTransform(Struct value, Map<String, ?> props) {
        final SourceRecord record = new SourceRecord(
                null, null, "test", 0,
                SchemaBuilder.bytes().optional().build(), "key".getBytes(), simpleSchema, value);

        DebeziumJsonDeserializer transformer = new DebeziumJsonDeserializer();
        transformer.configure(props);

        return transformer.apply(record);
    }

    private SourceRecord doTransform(Struct value) {
        return doTransform(value, new HashMap<>());
    }

    @Test
    public void transformsAnArrayOfStrings() {
        Struct value = new Struct(simpleSchema);
        value.put("id", "1234-5678");
        value.put("json", "[\"foo\", \"bar\"]");

        final SourceRecord transformedRecord = doTransform(value);

        Schema transformedValueSchema = transformedRecord.valueSchema();

        assertNotNull(transformedValueSchema);
        assertNotNull(transformedValueSchema.field("json"));
        assertEquals(Schema.Type.ARRAY, transformedValueSchema.field("json").schema().type());
        assertEquals(Schema.Type.STRING, transformedValueSchema.field("json").schema().valueSchema().type());
    }

    @Test
    public void ignoresANullValue() {
        Struct value = new Struct(simpleSchema);
        value.put("id", "1234-5678");
        value.put("json", null);

        final SourceRecord transformedRecord = doTransform(value);

        Schema transformedValueSchema = transformedRecord.valueSchema();

        assertNotNull(transformedValueSchema);
        assertNotNull(transformedValueSchema.field("id"));
        assertNull(transformedValueSchema.field("json"));
    }

    @Test
    public void ignoresANullValueWithinProperty() {
        Struct value = new Struct(simpleSchema);
        value.put("id", "1234-5678");
        value.put("json", "{\"foo\": \"bar\", \"baz\": null}");

        final SourceRecord transformedRecord = doTransform(value);

        Schema transformedValueSchema = transformedRecord.valueSchema();

        assertNotNull(transformedValueSchema);
        assertNotNull(transformedValueSchema.field("id"));
        assertNotNull(transformedValueSchema.field("json"));

        Schema jsonSchema = transformedValueSchema.field("json").schema();
        assertEquals(Schema.Type.STRUCT, jsonSchema.type());
        assertNotNull(jsonSchema.field("foo"));
        assertNull(jsonSchema.field("baz"));
    }

    @Test
    public void transformsAStruct() {
        Struct value = new Struct(simpleSchema);
        value.put("id", "1234-5678");
        value.put("json", "{\"foo\": \"bar\", \"baz\": 10, \"plop\": [\"a\", \"b\"]}");

        final SourceRecord transformedRecord = doTransform(value);

        Schema transformedValueSchema = transformedRecord.valueSchema();

        assertNotNull(transformedValueSchema);
        assertNotNull(transformedValueSchema.field("json"));

        Schema jsonSchema = transformedValueSchema.field("json").schema();
        assertEquals(Schema.Type.STRUCT, jsonSchema.type());
        assertNotNull(jsonSchema.field("foo"));
        assertNotNull(jsonSchema.field("baz"));
        assertNotNull(jsonSchema.field("plop"));

        assertEquals(Schema.Type.ARRAY, jsonSchema.field("plop").schema().type());
    }

    @Test
    public void transformsAnArrayOfStruct() {
        Struct value = new Struct(simpleSchema);
        value.put("id", "1234-5678");
        value.put("json", "{\n" +
                "  \"field1\": [{\"id\": 1}]\n" +
                "  \"field2\": [{\"id\": 2}, {\"id\": 3}]\n" +
                "}");

        final SourceRecord transformedRecord = doTransform(value);

        Schema jsonSchema = transformedRecord.valueSchema().field("json").schema();
        assertEquals(Schema.Type.STRUCT, jsonSchema.type());
        assertEquals(Schema.Type.ARRAY, jsonSchema.field("field1").schema().type());
        assertEquals(jsonSchema.field("field1").schema().valueSchema().fields(), jsonSchema.field("field2").schema().valueSchema().fields());
    }

    @Test
    public void transformsAnArrayOfDifferentStructsWithRequiredCommonFields() {
        Struct value = new Struct(simpleSchema);
        value.put("id", "1234-5678");
        value.put("json", "[\n" +
                "  {\"type\": \"care_task\", \"id\": \"48385242-96d5-11eb-b8f1-4fc97a48a234\", \"note\": \"My note\", \"task_definition_id\": \"1234\"},\n" +
                "  {\"type\": \"regular_task\", \"id\": \"502951c2-96d5-11eb-8776-33a3a6b06ce7\", \"external_schedule_id\": \"123\", \"time_of_day\": \"MORNING\", \"execution_offset\": 3600}\n" +
                "]");

        final SourceRecord transformedRecord = doTransform(value);

        Schema transformedValueSchema = transformedRecord.valueSchema();

        assertNotNull(transformedValueSchema);
        assertNotNull(transformedValueSchema.name());
        assertNotNull(transformedValueSchema.field("json"));

        Schema jsonSchema = transformedValueSchema.field("json").schema();
        assertEquals(Schema.Type.ARRAY, jsonSchema.type());
        assertEquals(Schema.Type.STRUCT, jsonSchema.valueSchema().type());

        assertNotNull(jsonSchema.valueSchema().field("type"));
        assertFalse(jsonSchema.valueSchema().field("type").schema().isOptional());

        assertNotNull(jsonSchema.valueSchema().field("id"));
        assertFalse(jsonSchema.valueSchema().field("id").schema().isOptional());

        assertNotNull(jsonSchema.valueSchema().field("note"));
        assertTrue(jsonSchema.valueSchema().field("note").schema().isOptional());
        assertEquals(Schema.Type.STRING, jsonSchema.valueSchema().field("note").schema().type());

        assertNotNull(jsonSchema.valueSchema().field("execution_offset"));
        assertTrue(jsonSchema.valueSchema().field("execution_offset").schema().isOptional());
        assertEquals(Schema.Type.INT64, jsonSchema.valueSchema().field("execution_offset").schema().type());
    }

    @Test
    public void transformsAnArrayOfDifferentStructsWithOptionalFields() {
        Struct value = new Struct(simpleSchema);
        value.put("id", "1234-5678");
        value.put("json", "[\n" +
                "  {\"type\": \"care_task\", \"id\": \"48385242-96d5-11eb-b8f1-4fc97a48a234\", \"note\": \"My note\", \"task_definition_id\": \"1234\"},\n" +
                "  {\"type\": \"regular_task\", \"id\": \"502951c2-96d5-11eb-8776-33a3a6b06ce7\", \"external_schedule_id\": \"123\", \"time_of_day\": \"MORNING\", \"execution_offset\": 3600}\n" +
                "]");

        final SourceRecord transformedRecord = doTransform(value, new HashMap<>() {{
           put("optional-struct-fields", "true");
        }});

        Schema transformedValueSchema = transformedRecord.valueSchema();

        Schema jsonSchema = transformedValueSchema.field("json").schema();
        assertTrue(jsonSchema.valueSchema().field("type").schema().isOptional());
        assertTrue(jsonSchema.valueSchema().field("id").schema().isOptional());
        assertTrue(jsonSchema.valueSchema().field("note").schema().isOptional());
        assertTrue(jsonSchema.valueSchema().field("execution_offset").schema().isOptional());
    }

    @Test
    public void transformsAJsonOfDifferentStructsWithOptionalFields() {
        Struct value = new Struct(simpleSchema);
        value.put("id", "1234-5678");
        value.put("json", "{\n" +
                "  \"field1\": {\"type\": \"care_task\", \"id\": \"48385242-96d5-11eb-b8f1-4fc97a48a234\", \"note\": \"My note\", \"task_definition_id\": \"1234\"},\n" +
                "  \"field2\": 100\n" +
                "}");

        final SourceRecord transformedRecord = doTransform(value, new HashMap<>() {{
           put("optional-struct-fields", "true");
        }});

        Schema transformedValueSchema = transformedRecord.valueSchema();

        Schema jsonSchema = transformedValueSchema.field("json").schema();
        assertTrue(jsonSchema.field("field1").schema().isOptional());
        assertTrue(jsonSchema.field("field2").schema().isOptional());
        assertTrue(jsonSchema.field("field1").schema().field("type").schema().isOptional());
    }

    @Test
    public void transformsEmptyArray() {
        Struct value = new Struct(simpleSchema);
        value.put("id", "1234-5678");
        value.put("json", "[]");

        final SourceRecord transformedRecord = doTransform(value);

        Schema transformedValueSchema = transformedRecord.valueSchema();
        Schema jsonSchema = transformedValueSchema.field("json").schema();
        assertEquals(Schema.Type.ARRAY, jsonSchema.type());
        assertEquals(Schema.Type.STRUCT, jsonSchema.valueSchema().type());
    }

    @Test
    public void transformsEmptyObject() {
        Struct value = new Struct(simpleSchema);
        value.put("id", "1234-5678");
        value.put("json", "{}");

        final SourceRecord transformedRecord = doTransform(value);

        Schema transformedValueSchema = transformedRecord.valueSchema();
        Schema jsonSchema = transformedValueSchema.field("json").schema();
        assertEquals(Schema.Type.STRUCT, jsonSchema.type());
    }

    @Test(expected = IllegalArgumentException.class)
    public void refuseArrayOfDifferentType() {
        Struct value = new Struct(simpleSchema);
        value.put("id", "1234-5678");
        value.put("json", "[\"a\", 12]}");

        doTransform(value);
    }
}
