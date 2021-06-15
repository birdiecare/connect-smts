package com.birdie.kafka.connect.smt;

import com.birdie.kafka.connect.utils.SchemaSerDer;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;
import uk.org.lidalia.slf4jtest.TestLogger;
import uk.org.lidalia.slf4jtest.TestLoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;


public class DebeziumJsonDeserializerTest {
    private static final Schema simpleSchema = SchemaBuilder.struct()
        .name("Value")
        .field("id", SchemaBuilder.STRING_SCHEMA)
        .field("json", SchemaBuilder.string().name("io.debezium.data.Json").optional().build())
        .build();

    public static SourceRecord doTransform(SourceRecord record, Map<String, ?> props) {
        DebeziumJsonDeserializer transformer = new DebeziumJsonDeserializer();
        transformer.configure(props);

        return transformer.apply(record);
    }

    public static SourceRecord doTransform(Struct value, Map<String, ?> props) {
        return doTransform(sourceRecordFromValue(value), props);
    }

    public static SourceRecord sourceRecordFromValue(Struct value) {
        return new SourceRecord(
                null, null, "a-database-name.public.the_database_table", 0,
                SchemaBuilder.bytes().optional().build(), "key".getBytes(), simpleSchema, value);
    }

    public static SourceRecord doTransform(Struct value) {
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

        Struct transformedValue = (Struct) transformedRecord.value();
        assertEquals("1234-5678", transformedValue.getString("id"));
        assertEquals("bar", transformedValue.getStruct("json").getString("foo"));
        assertEquals(10, transformedValue.getStruct("json").get("baz"));
        assertEquals("a", transformedValue.getStruct("json").getArray("plop").get(0));
        assertEquals("b", transformedValue.getStruct("json").getArray("plop").get(1));
    }

    @Test
    public void transformsAnArrayOfStruct() {
        Struct value = new Struct(simpleSchema);
        value.put("id", "1234-5678");
        value.put("json", "{\n" +
                "  \"field1\": [{\"id\": 1}],\n" +
                "  \"field2\": [{\"id\": 2}, {\"id\": 3}]\n" +
                "}");

        final SourceRecord transformedRecord = doTransform(value);

        Schema jsonSchema = transformedRecord.valueSchema().field("json").schema();
        assertEquals(Schema.Type.STRUCT, jsonSchema.type());
        assertEquals(Schema.Type.ARRAY, jsonSchema.field("field1").schema().type());
        assertEquals(jsonSchema.field("field1").schema().valueSchema().fields(), jsonSchema.field("field2").schema().valueSchema().fields());
    }

    @Test
    public void transformsAnArrayOfNumbersAndAnEmptyNumberTogether() {
        Struct value = new Struct(simpleSchema);
        value.put("id", "1234-5678");
        value.put("json", "{\n" +
                "  \"field1\": [{\"ids\": [1, 2, 3]}, {\"ids\": []}]\n" +
                "}");

        final SourceRecord transformedRecord = doTransform(value);

        Schema jsonSchema = transformedRecord.valueSchema().field("json").schema();
        assertEquals(Schema.Type.STRUCT, jsonSchema.type());
        assertEquals(Schema.Type.ARRAY, jsonSchema.field("field1").schema().type());
        assertNotNull(jsonSchema.field("field1").schema().valueSchema().field("ids"));
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
        assertEquals(Schema.Type.INT32, jsonSchema.valueSchema().field("execution_offset").schema().type());
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
    public void ignoresEmptyArrays() {
        Struct value = new Struct(simpleSchema);
        value.put("id", "1234-5678");
        value.put("json", "[]");

        final SourceRecord transformedRecord = doTransform(value);

        Schema transformedValueSchema = transformedRecord.valueSchema();

        assertNull(transformedValueSchema.field("json"));
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

    @Test
    public void ignoresEmptyJsonValue() {
        Struct value = new Struct(simpleSchema);
        value.put("id", "1234-5678");
        value.put("json", "");

        final SourceRecord transformedRecord = doTransform(value);

        Schema transformedValueSchema = transformedRecord.valueSchema();
        assertNull(transformedValueSchema.field("json"));
    }

    @Test
    public void transformsEmptyJsonString() {
        Struct value = new Struct(simpleSchema);
        value.put("id", "1234-5678");
        value.put("json", "\"\"");

        final SourceRecord transformedRecord = doTransform(value);

        Schema transformedValueSchema = transformedRecord.valueSchema();
        assertNotNull(transformedValueSchema.field("json"));
        assertEquals(Schema.Type.STRING, transformedValueSchema.field("json").schema().type());
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
                "  {\"id\": 1, \"temperature\": 37.5}\n" +
                "]");

        final SourceRecord transformedRecord = doTransform(value);

        Schema transformedValueSchema = transformedRecord.valueSchema();

        Schema jsonSchema = transformedValueSchema.field("json").schema();
        assertEquals(Schema.Type.INT32, jsonSchema.valueSchema().field("id").schema().type());
        assertEquals(Schema.Type.FLOAT64, jsonSchema.valueSchema().field("temperature").schema().type());
    }

    @Test
    public void transformsIntegersIntoFloatsWithConvertOption() {
        Struct value = new Struct(simpleSchema);
        value.put("id", "1234-5678");
        value.put("json", "{\"temperature\": 37}");

        final SourceRecord transformedRecord = doTransform(value, new HashMap<>() {{
           put("convert-numbers-to-double", "true");
        }});

        Schema transformedValueSchema = transformedRecord.valueSchema();

        assertEquals(Schema.Type.FLOAT64, transformedValueSchema.field("json").schema().field("temperature").schema().type());

        Struct transformedValue = (Struct) transformedRecord.value();
        assertEquals(Double.valueOf(37.0), transformedValue.getStruct("json").getFloat64("temperature"));
    }

    @Test
    public void itDoesProduceValidAvroNamesFromJsonProperties() {
        Struct value = new Struct(simpleSchema);
        value.put("id", "1234-5678");
        value.put("json", "{\"with space\": 10, \"1some_details\":{\"sub key\": \"plop\", \"_childrenNavigation\":{\"id-1611255248160-29\":{\"state\":{\"params\":{\"recipientId\":\"cd1a413e-446c-50bd-8b74-5e59606f383d\"}}}}}}");

        final SourceRecord transformedRecord = doTransform(value, new HashMap<>() {{
            put("sanitize.field.names", "true");
        }});

        Schema transformedValueSchema = transformedRecord.valueSchema();

        assertNotNull(transformedValueSchema);
        assertNotNull(transformedValueSchema.field("json"));

        Schema jsonSchema = transformedValueSchema.field("json").schema();

        assertEquals(Schema.Type.STRUCT, jsonSchema.type());
        assertNotNull(jsonSchema.field("with_space"));
        assertNotNull(jsonSchema.field("_1some_details"));
        assertEquals("json__1some_details", jsonSchema.field("_1some_details").schema().name());
        assertNotNull(jsonSchema.field("_1some_details").schema().field("sub_key"));
        assertNotNull(jsonSchema.field("_1some_details").schema().field("_childrenNavigation"));
        assertNotNull(jsonSchema.field("_1some_details").schema().field("_childrenNavigation").schema().field("id_1611255248160_29"));
        assertEquals("json__1some_details__childrenNavigation_id_1611255248160_29", jsonSchema.field("_1some_details").schema().field("_childrenNavigation").schema().field("id_1611255248160_29").schema().name());
    }

    @Test
    public void transformsArraysWithinNestedArrays() {
        Struct value = new Struct(simpleSchema);
        value.put("id", "1234-5678");
        value.put("json", "[{\"id\": 1, \"values\": [1, 2]}, {\"id\": 2, \"values\": [3, 4]}]");

        final SourceRecord transformedRecord = doTransform(value, new HashMap<>() {{
            put("optional-struct-fields", "true");
        }});

        Schema transformedValueSchema = transformedRecord.valueSchema();

        assertNotNull(transformedValueSchema);
        assertNotNull(transformedValueSchema.field("json"));
        assertNotNull(transformedValueSchema.field("json").schema().valueSchema().field("id"));
        assertNotNull(transformedValueSchema.field("json").schema().valueSchema().field("values"));
    }

    @Test
    public void transformsArraysWithinNestedStructures() {
        Struct value = new Struct(simpleSchema);
        value.put("id", "1234-5678");
        value.put("json", "[{\"id\": 1, \"values\": {\"field1\": 1, \"field2\": [1, 2]}}, {\"id\": 2, \"values\": {\"field1\": 2, \"field2\": [1, 2, 3]}}]");

        final SourceRecord transformedRecord = doTransform(value);
        Schema transformedValueSchema = transformedRecord.valueSchema();

        assertNotNull(transformedValueSchema);
        assertNotNull(transformedValueSchema.field("json"));
        assertNotNull(transformedValueSchema.field("json").schema().valueSchema().field("values"));
        assertNotNull(transformedValueSchema.field("json").schema().valueSchema().field("values").schema().field("field1"));
        assertNotNull(transformedValueSchema.field("json").schema().valueSchema().field("values").schema().field("field2"));
    }

    @Test
    public void transformsNestedStructuresWithDifferentChildrenStructureWithinArrays() {
        Struct value = new Struct(simpleSchema);
        value.put("id", "1234-5678");
        value.put("json", "{\"reason\":\"ValidationError: id, care_recipient_id\",\"validation_errors\":[{\"property\":\"id\",\"value\":\"7e70ffab-1200-1300--43894\",\"constraints\":{\"isUuid\":\"id must be an UUID\"}},{\"property\":\"care_recipient_id\",\"value\":\"\",\"constraints\":{\"isUuid\":\"care_recipient_id must be an UUID\",\"isNotEmpty\":\"care_recipient_id should not be empty\"}}],\"id\":\"3addd635-8df6-4c32-8035-6c328fb0fb27\",\"timestamp\":\"2020-02-28T11:00:30.987Z\",\"event_type\":\"visit_synchronisation_failed\",\"agency_id\":\"a4bb5719-96f6-4d2c-ac66-44a2e9c6a014\",\"remote_id\":\"7e70ffab-1200-1300--43894\"}");

        final SourceRecord transformedRecord = doTransform(value);
        Schema transformedValueSchema = transformedRecord.valueSchema();

        assertNotNull(transformedValueSchema);
        assertNotNull(transformedValueSchema.field("json"));
    }

    @Test
    public void transformsDifferentListItemsWithinStructWithinLists() {
        Struct value = new Struct(simpleSchema);
        value.put("id", "1234-5678");
        value.put("json", "{\"a_list\": [{\"another_list\": []}, {\"another_list\": [{\"foo\": \"bar\"}]}]}");

        final SourceRecord transformedRecord = doTransform(value);
        Schema transformedValueSchema = transformedRecord.valueSchema();

        assertNotNull(transformedValueSchema);
        assertNotNull(transformedValueSchema.field("json"));
    }

    @Test
    public void skipsTombstones() {
        final SourceRecord record = new SourceRecord(
                null, null, "test", 0,
                SchemaBuilder.bytes().optional().build(), "key".getBytes(), null, null);

        final SourceRecord transformedRecord = doTransform(record, new HashMap<>());

        assertEquals(record, transformedRecord);
    }

    @Test
    public void unionSchemasAcrossMultipleMessages() {
        Struct firstMessageContents = new Struct(simpleSchema);
        firstMessageContents.put("id", "1234-5678");
        firstMessageContents.put("json", "{\"foo\": \"da value\"}");

        Struct secondMessageContents = new Struct(simpleSchema);
        secondMessageContents.put("id", "1234-5678");
        secondMessageContents.put("json", "{\"bar\": \"oh a value\"}");

        Struct thirdMessageContents = new Struct(simpleSchema);
        thirdMessageContents.put("id", "1234-5678");
        thirdMessageContents.put("json", "{\"foo\": \"way\", \"bar\": \"plop\"}");

        Struct fourthMessageContents = new Struct(simpleSchema);
        fourthMessageContents.put("id", "1234-5678");
        fourthMessageContents.put("json", "{\"foo\": \"way\", \"baz\": {\"one\": 1}}");

        DebeziumJsonDeserializer transformer = new DebeziumJsonDeserializer();
        transformer.configure(new HashMap<>() {{
            put("optional-struct-fields", "true");
            put("union-previous-messages-schema", "true");
        }});

        SourceRecord firstTransformed = transformer.apply(sourceRecordFromValue(firstMessageContents));
        SourceRecord secondTransformed = transformer.apply(sourceRecordFromValue(secondMessageContents));
        SourceRecord thirdTransformed = transformer.apply(sourceRecordFromValue(thirdMessageContents));
        SourceRecord fourthTransformed = transformer.apply(sourceRecordFromValue(fourthMessageContents));

        assertNotNull(firstTransformed.valueSchema().field("json").schema().field("foo"));
        assertNull(firstTransformed.valueSchema().field("json").schema().field("bar"));

        assertNotNull(secondTransformed.valueSchema().field("json").schema().field("foo"));
        assertNotNull(secondTransformed.valueSchema().field("json").schema().field("bar"));

        assertEqualsSchemas(secondTransformed.valueSchema(), thirdTransformed.valueSchema());

        assertNotNull(fourthTransformed.valueSchema().field("json").schema().field("foo"));
        assertNotNull(fourthTransformed.valueSchema().field("json").schema().field("bar"));
        assertNotNull(fourthTransformed.valueSchema().field("json").schema().field("baz"));
    }

    @Test
    public void handlesEmptyArraysWhenUninioingSchemas() {
        Struct firstMessageContents = new Struct(simpleSchema);
        firstMessageContents.put("id", "1234-5678");
        firstMessageContents.put("json", "[]");

        DebeziumJsonDeserializer transformer = new DebeziumJsonDeserializer();
        transformer.configure(new HashMap<>() {{
            put("optional-struct-fields", "true");
            put("union-previous-messages-schema", "true");
        }});

        transformer.apply(sourceRecordFromValue(firstMessageContents));
    }

    @Test
    public void itLogsSignificantStepsOfTheProcessAndSerializesTheCurrentSchema() throws JsonProcessingException {
        SchemaSerDer schemaSerDer = new SchemaSerDer();
        TestLogger logger = TestLoggerFactory.getTestLogger(DebeziumJsonDeserializer.class);
        DebeziumJsonDeserializer transformer = new DebeziumJsonDeserializer();
        transformer.configure(new HashMap<>() {{
            put("optional-struct-fields", "true");
            put("union-previous-messages-schema", "true");
        }});

        logger.clear();
        assertEquals(0, logger.getLoggingEvents().size());

        // Create a new schema
        Struct firstMessageContents = new Struct(simpleSchema);
        firstMessageContents.put("id", "1234-5678");
        firstMessageContents.put("json", "{\"foo\": \"da value\"}");
        SourceRecord transformed = transformer.apply(sourceRecordFromValue(firstMessageContents));

        String logMessage = logger.getLoggingEvents().get(0).getMessage();
        assertTrue(logMessage.startsWith("Registering schema json#0"));

        String serializedSchema = logMessage.substring(logMessage.indexOf(':') + 2);
        Schema deserializedFromLog = schemaSerDer.deserializeOne(serializedSchema);
        assertEqualsSchemas(deserializedFromLog, transformed.valueSchema().field("json").schema());

        logger.clear();

        // Updates a schema
        Struct secondMessageContents = new Struct(simpleSchema);
        secondMessageContents.put("id", "1234-5678");
        secondMessageContents.put("json", "{\"bar\": \"oh a value\"}");
        transformed = transformer.apply(sourceRecordFromValue(secondMessageContents));

        logMessage = logger.getLoggingEvents().get(0).getMessage();

        assertTrue(logMessage.startsWith("Updating schema json#0"));
        serializedSchema = logMessage.substring(logMessage.indexOf(':') + 2);
        deserializedFromLog = schemaSerDer.deserializeOne(serializedSchema);
        assertEqualsSchemas(deserializedFromLog, transformed.valueSchema().field("json").schema());

        logger.clear();

        // Re-uses a schema
        Struct thirdMessageContents = new Struct(simpleSchema);
        thirdMessageContents.put("id", "1234-5678");
        thirdMessageContents.put("json", "{\"foo\": \"way\", \"bar\": \"plop\"}");
        transformer.apply(sourceRecordFromValue(thirdMessageContents));

        assertEquals(0, logger.getLoggingEvents().size());
        logger.clear();
    }

    @Test
    public void unionSchemasAcrossMultipleIncompatibleMessages() {
        Struct firstMessageContents = new Struct(simpleSchema);
        firstMessageContents.put("id", "1234-5678");
        firstMessageContents.put("json", "{\"foo\": \"da value\", \"bar\": \"somethingElse\"}");

        Struct secondMessageContents = new Struct(simpleSchema);
        secondMessageContents.put("id", "1234-5678");
        secondMessageContents.put("json", "{\"foo\": [\"oh a value\"]}");

        Struct thirdMessageContents = new Struct(simpleSchema);
        thirdMessageContents.put("id", "1234-5678");
        thirdMessageContents.put("json", "{\"foo\": \"way\"}");

        Struct fourthMessageContents = new Struct(simpleSchema);
        fourthMessageContents.put("id", "1234-5678");
        fourthMessageContents.put("json", "[{\"un\": 1}]");

        Struct fifthMessageContents = new Struct(simpleSchema);
        fifthMessageContents.put("id", "1234-5678");
        fifthMessageContents.put("json", "[{\"deux\": 1}]");

        DebeziumJsonDeserializer transformer = new DebeziumJsonDeserializer();
        transformer.configure(new HashMap<>() {{
            put("optional-struct-fields", "true");
            put("union-previous-messages-schema", "true");
        }});

        SourceRecord firstTransformed = transformer.apply(sourceRecordFromValue(firstMessageContents));
        SourceRecord secondTransformed = transformer.apply(sourceRecordFromValue(secondMessageContents));
        SourceRecord thirdTransformed = transformer.apply(sourceRecordFromValue(thirdMessageContents));
        SourceRecord fourthTransformed = transformer.apply(sourceRecordFromValue(fourthMessageContents));
        SourceRecord fifthTransformed = transformer.apply(sourceRecordFromValue(fifthMessageContents));

        // Second message is its own, an array for foo
        assertNotNull(secondTransformed.valueSchema().field("json").schema().field("foo"));
        assertNull(secondTransformed.valueSchema().field("json").schema().field("bar"));

        // First and 2nd have the same schema, with foo and bar
        assertEqualsSchemas(firstTransformed.valueSchema(), thirdTransformed.valueSchema());
        assertEquals(Schema.Type.ARRAY, fourthTransformed.valueSchema().field("json").schema().type());

        assertNotNull(fifthTransformed.valueSchema().field("json").schema().valueSchema().field("un"));
        assertNotNull(fifthTransformed.valueSchema().field("json").schema().valueSchema().field("deux"));
    }

    @Test
    public void handlesEmptyValuesForBasicFields() {
        Struct firstMessageContents = new Struct(simpleSchema);
        firstMessageContents.put("id", "1234-5678");
        firstMessageContents.put("json", "[{\"bar\": \"da value\", \"foo\": [{\"meh\": \"yay\"}]}]");

        Struct secondMessageContents = new Struct(simpleSchema);
        secondMessageContents.put("id", "1234-5678");
        secondMessageContents.put("json", "[{\"foo\":[null]}]");

        DebeziumJsonDeserializer transformer = new DebeziumJsonDeserializer();
        transformer.configure(new HashMap<>() {{
            put("optional-struct-fields", "true");
            put("union-previous-messages-schema", "true");
        }});

        transformer.apply(sourceRecordFromValue(firstMessageContents));
        SourceRecord secondTransformed = transformer.apply(sourceRecordFromValue(secondMessageContents));

        assertNotNull(secondTransformed.valueSchema().field("json").schema().valueSchema().field("foo"));
        assertNotNull(secondTransformed.valueSchema().field("json").schema().valueSchema().field("bar"));
    }

    @Test
    public void usesAnInitialSchemaForAField() {
        Struct firstMessageContents = new Struct(simpleSchema);
        firstMessageContents.put("id", "1234-5678");
        firstMessageContents.put("json", "{\"something_else\": 1}");

        DebeziumJsonDeserializer transformer = new DebeziumJsonDeserializer();
        transformer.configure(new HashMap<>() {{
            put("optional-struct-fields", "true");
            put("union-previous-messages-schema", "true");
            put("union-previous-messages-schema.topic.a-database-name.public.the_database_table.field.json", "[{\"type\":\"STRUCT\",\"name\":\"json\",\"isOptional\":true,\"fields\":[{\"name\":\"bar\",\"index\":0,\"schema\":{\"type\":\"STRING\",\"isOptional\":true}},{\"name\":\"foo\",\"index\":1,\"schema\":{\"type\":\"STRING\",\"isOptional\":true}}]}]");
        }});

        SourceRecord transformed = transformer.apply(sourceRecordFromValue(firstMessageContents));

        assertNotNull(transformed.valueSchema().field("json").schema().field("foo"));
        assertNotNull(transformed.valueSchema().field("json").schema().field("bar"));
        assertNotNull(transformed.valueSchema().field("json").schema().field("something_else"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsWhenTheFormatOfTheFieldConfigurationKeyIsInvalid() {
        DebeziumJsonDeserializer transformer = new DebeziumJsonDeserializer();
        transformer.configure(new HashMap<>() {{
            put("optional-struct-fields", "true");
            put("union-previous-messages-schema", "true");
            put("union-previous-messages-schema.topic.a-database-name.public.the_database_table.json", "[{\"type\":\"STRUCT\",\"name\":\"json\",\"isOptional\":true,\"fields\":[{\"name\":\"bar\",\"index\":0,\"schema\":{\"type\":\"STRING\",\"isOptional\":true}},{\"name\":\"foo\",\"index\":1,\"schema\":{\"type\":\"STRING\",\"isOptional\":true}}]}]");
        }});
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsWhenTheFieldSchemaIsAWrongSyntax() {
        DebeziumJsonDeserializer transformer = new DebeziumJsonDeserializer();
        transformer.configure(new HashMap<>() {{
            put("optional-struct-fields", "true");
            put("union-previous-messages-schema", "true");
            put("union-previous-messages-schema.topic.a-database-name.public.the_database_table.field.json", "{\"type\":\"STRUCT\",\"name\":\"json\",\"isOptional\":true}");
        }});
    }

    @Test
    public void usesTheFastPathWhenPossible() {
        Struct firstMessageContents = new Struct(simpleSchema);
        firstMessageContents.put("id", "1234-5678");
        firstMessageContents.put("json", "{\"foo\": \"yay\"}");

        DebeziumJsonDeserializer transformer = new DebeziumJsonDeserializer();
        transformer.configure(new HashMap<>() {{
            put("optional-struct-fields", "true");
            put("union-previous-messages-schema", "true");
            put("union-previous-messages-schema.topic.a-database-name.public.the_database_table.field.json", "[{\"type\":\"STRUCT\",\"name\":\"json\",\"isOptional\":true,\"fields\":[{\"name\":\"bar\",\"index\":0,\"schema\":{\"type\":\"STRING\",\"isOptional\":true}},{\"name\":\"foo\",\"index\":1,\"schema\":{\"type\":\"STRING\",\"isOptional\":true}}]}]");
            put("probabilistic-fast-path", "true");
        }});

        SourceRecord transformed = transformer.apply(sourceRecordFromValue(firstMessageContents));

        assertNotNull(transformed.valueSchema().field("json").schema().field("foo"));
        assertNotNull(transformed.valueSchema().field("json").schema().field("bar"));
    }

    @Test
    public void fallsBackOnUnionIfFastTrackFails() {
        Struct firstMessageContents = new Struct(simpleSchema);
        firstMessageContents.put("id", "1234-5678");
        firstMessageContents.put("json", "{\"baz\": \"plop\"}");

        DebeziumJsonDeserializer transformer = new DebeziumJsonDeserializer();
        transformer.configure(new HashMap<>() {{
            put("optional-struct-fields", "true");
            put("union-previous-messages-schema", "true");
            put("union-previous-messages-schema.topic.a-database-name.public.the_database_table.field.json", "[{\"type\":\"STRUCT\",\"name\":\"json\",\"isOptional\":true,\"fields\":[{\"name\":\"bar\",\"index\":0,\"schema\":{\"type\":\"STRING\",\"isOptional\":true}},{\"name\":\"foo\",\"index\":1,\"schema\":{\"type\":\"STRING\",\"isOptional\":true}}]}]");
            put("probabilistic-fast-path", "true");
        }});

        SourceRecord transformed = transformer.apply(sourceRecordFromValue(firstMessageContents));

        assertNotNull(transformed.valueSchema().field("json").schema().field("foo"));
        assertNotNull(transformed.valueSchema().field("json").schema().field("bar"));
        assertNotNull(transformed.valueSchema().field("json").schema().field("baz"));
    }

    public static void assertEqualsSchemas(Schema left, Schema right) {
        assertEquals(left.name(), right.name());
        assertEquals(left.isOptional(), right.isOptional());
        assertEquals(left.type(), right.type());
        assertEquals(left.defaultValue(), right.defaultValue());

        if (left.type().equals(Schema.Type.STRUCT)) {
            for (Field leftField : left.fields()) {
                Field rightField = right.field(leftField.name());

                assertNotNull(rightField);
                assertEquals(leftField.name(), rightField.name());
                assertEqualsSchemas(leftField.schema(), rightField.schema());
            }
        }

        if (left.type().equals(Schema.Type.ARRAY)) {
            assertEqualsSchemas(left.valueSchema(), right.valueSchema());
        }
    }

    @Test
    public void ignoresSelectedFields() {
        Struct firstMessageContents = new Struct(simpleSchema);
        firstMessageContents.put("id", "1234-5678");
        firstMessageContents.put("json", "{\"foo\": \"yay\", \"baz\": \"plop\",  \"ignored\": {\"yes\": 1, \"no\": 0}}");

        DebeziumJsonDeserializer transformer = new DebeziumJsonDeserializer();
        transformer.configure(new HashMap<>() {{
            put("optional-struct-fields", "true");
            put("ignored-fields", "json.baz,json.ignored.yes");
        }});

        SourceRecord transformed = transformer.apply(sourceRecordFromValue(firstMessageContents));

        assertNotNull(transformed.valueSchema().field("json").schema().field("foo"));
        assertNull(transformed.valueSchema().field("json").schema().field("baz"));
        assertNull(transformed.valueSchema().field("json").schema().field("ignored").schema().field("yes"));
        assertNotNull(transformed.valueSchema().field("json").schema().field("ignored").schema().field("no"));
    }
}
