package com.birdie.kafka.connect.json;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

import static org.junit.Assert.*;

public class SchemaTransformerTest {
    @Test
    public void twoIdenticalSchemasWithAnArrayMergedTogetherRemainTheSameStructure() {
        Schema left = SchemaBuilder.struct()
                .name("Value")
                .field("id", SchemaBuilder.STRING_SCHEMA)
                .field("array_of_ints", SchemaBuilder.array(SchemaBuilder.int8()))
                .build();

        Schema merged = new SchemaTransformer(false, false, false).unionSchemas(left, left).build();

        assertEquals("Value", merged.name());
        assertNotNull(merged.field("id"));
        assertEquals(Schema.Type.STRING, merged.field("id").schema().type());
        assertNotNull(merged.field("array_of_ints"));
        assertEquals(Schema.Type.ARRAY, merged.field("array_of_ints").schema().type());
        assertEquals(Schema.Type.INT8, merged.field("array_of_ints").schema().valueSchema().type());
    }

    @Test
    public void mergesNestedStructures() {
        Schema left = SchemaBuilder.struct()
                .name("Value")
                .field("id", SchemaBuilder.STRING_SCHEMA)
                .field("nested", SchemaBuilder.struct()
                    .field("foo", SchemaBuilder.STRING_SCHEMA)
                )
                .build();

        Schema right = SchemaBuilder.struct()
                .name("Value")
                .field("id", SchemaBuilder.STRING_SCHEMA)
                .field("nested", SchemaBuilder.struct()
                        .field("bar", SchemaBuilder.STRING_SCHEMA)
                )
                .build();

        Schema merged = new SchemaTransformer(true, false, false).unionSchemas(left, right).build();

        assertNotNull(merged.field("nested"));
        assertNotNull(merged.field("nested").schema().field("bar"));
        assertNotNull(merged.field("nested").schema().field("foo"));
    }

    @Test
    public void keepsADeterministicOrderForKeys() {
        Schema left = SchemaBuilder.struct()
                .name("Value")
                .field("foo", SchemaBuilder.STRING_SCHEMA)
                .field("baz", SchemaBuilder.struct()
                        .field("sam", SchemaBuilder.STRING_SCHEMA)
                        .field("uel", SchemaBuilder.STRING_SCHEMA)
                )
                .build();

        Schema right = SchemaBuilder.struct()
                .name("Value")
                .field("id", SchemaBuilder.STRING_SCHEMA)
                .field("baz", SchemaBuilder.struct()
                        .field("uel", SchemaBuilder.STRING_SCHEMA)
                        .field("sam", SchemaBuilder.STRING_SCHEMA)
                )
                .field("foo", SchemaBuilder.STRING_SCHEMA)
                .build();

        SchemaTransformer transformer = new SchemaTransformer(true, true, true);

        assertEquals(transformer.unionSchemas(left, right, left).build(), transformer.unionSchemas(right, left, right).build());
    }
}
