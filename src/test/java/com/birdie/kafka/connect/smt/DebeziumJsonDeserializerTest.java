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

    @Test
    public void leavesIntegersWithoutConvertOption() {
        Struct value = new Struct(simpleSchema);
        value.put("id", "1234-5678");
        value.put("json", "[\n" +
                "  {\"id\": 1, \"temperature\": 37.5},\n" +
                "]");

        final SourceRecord transformedRecord = doTransform(value);

        Schema transformedValueSchema = transformedRecord.valueSchema();

        Schema jsonSchema = transformedValueSchema.field("json").schema();
        assertEquals(Schema.Type.INT64, jsonSchema.valueSchema().field("id").schema().type());
        assertEquals(Schema.Type.FLOAT64, jsonSchema.valueSchema().field("temperature").schema().type());
    }

    @Test
    public void transformsIntegersIntoFloatsWithConvertOption() {
        Struct value = new Struct(simpleSchema);
        value.put("id", "1234-5678");
        value.put("json", "[\n" +
        "  {\"id\": 1, \"temperature\": 37.5},\n" +
        "]");

        final SourceRecord transformedRecord = doTransform(value, new HashMap<>() {{
           put("convert-numbers-to-double", "true");
        }});

        Schema transformedValueSchema = transformedRecord.valueSchema();

        Schema arraySchema = transformedValueSchema.field("json").schema();
        assertEquals(Schema.Type.FLOAT64, arraySchema.valueSchema().field("id").schema().type());
        assertEquals(Schema.Type.FLOAT64, arraySchema.valueSchema().field("temperature").schema().type());
    }

    @Test
    public void itDoesProduceValidAvroNamesFromJsonProperties() {
        Struct value = new Struct(simpleSchema);
        value.put("id", "1234-5678");
        value.put("json", "{\"with space\": 10, \"1some_details\":{\"sub key\": \"plop\"}}");

        final SourceRecord transformedRecord = doTransform(value, new HashMap<>() {{
            put("sanitize.field.names", "true");
        }});

        Schema transformedValueSchema = transformedRecord.valueSchema();

        assertNotNull(transformedValueSchema);
        assertNotNull(transformedValueSchema.field("json"));

        Schema jsonSchema = transformedValueSchema.field("json").schema();

        assertEquals(Schema.Type.STRUCT, jsonSchema.type());
        assertNotNull(jsonSchema.field("with_space"));
        assertEquals("with_space", jsonSchema.field("with_space").name());
        assertNotNull(jsonSchema.field("_1some_details"));
        assertNotNull(jsonSchema.field("_1some_details").schema().field("sub_key"));
    }

    @Test
    public void doesNotThrowNullException() {
        Struct value = new Struct(simpleSchema);
        value.put("id", "1234-5678");
        value.put("json", "[{\"id\": 1, \"values\": [1, 2]}, {\"id\": 2, \"values\": [3, 4]}]");

        final SourceRecord transformedRecord = doTransform(value);
        Schema transformedValueSchema = transformedRecord.valueSchema();

        assertNotNull(transformedValueSchema);
        assertNotNull(transformedValueSchema.field("json"));
    }

    @Test
    public void structSchemasDoNotMatch() {
        Struct value = new Struct(simpleSchema);
        value.put("id", "1234-5678");
        value.put("json", "[{\"id\": 1, \"values\": {\"field1\": 1}}, {\"id\": 2, \"values\": {\"field1\": 2}}]");

        final SourceRecord transformedRecord = doTransform(value);
        Schema transformedValueSchema = transformedRecord.valueSchema();

        assertNotNull(transformedValueSchema);
        assertNotNull(transformedValueSchema.field("json"));
    }
}

// {"reason":"ValidationError: id, care_recipient_id","validation_errors":[{"property":"id","value":"1099156","constraints":{"isUuid":"id must be an UUID"}},{"property":"care_recipient_id","value":"2510","constraints":{"isUuid":"care_recipient_id must be an UUID"}}],"id":"8d90d51a-f1cd-481b-bacb-29fcfd119177","timestamp":"2020-10-02T09:01:47.177Z","event_type":"visit_synchronisation_failed","agency_id":"21483479-6acb-4e50-89cd-e21e039dd120","remote_id":"1099156"}