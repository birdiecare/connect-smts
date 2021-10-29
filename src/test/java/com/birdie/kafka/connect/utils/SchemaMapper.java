package com.birdie.kafka.connect.utils;

import com.birdie.kafka.connect.json.SchemaTransformer;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.util.ArrayList;
import java.util.List;

class SchemaMapperBenchMark {
    SchemaTransformer schemaTransformer;

    public SchemaMapperBenchMark(SchemaTransformer schemaTransformer)
    {
        this.schemaTransformer = schemaTransformer;
    }

    public Object mapJsonToSchemaWithoutCheckTypes(Schema schema, JsonNode json) {
        if (json == null || json.isNull()) {
            if (schema.isOptional()) {
                return null;
            }

            throw new IllegalArgumentException("Empty value for non optional field.");
        }

        if (schema.type().equals(Schema.Type.STRUCT)) {
            if (!json.isObject()) {
                throw new IllegalArgumentException("Expected an object to map to a structure.");
            }

            Struct struct = new Struct(schema);
            json.fields().forEachRemaining(field -> {
                JsonNode fieldValue = field.getValue();
                if (fieldValue == null || fieldValue.isNull()) {
                    return;
                }

                Field fieldInSchema = schema.field(field.getKey());
                if (fieldInSchema == null) {
                    throw new IllegalArgumentException("Field "+field.getKey()+" does not seem to exist here.");
                }

                struct.put(field.getKey(), mapJsonToSchemaWithoutCheckTypes(
                    fieldInSchema.schema(),
                    fieldValue
                ));
            });

            return struct;
        } else if (schema.type().equals(Schema.Type.ARRAY)) {
            if (!json.isArray()) {
                throw new IllegalArgumentException("Expected an array to map to an array.");
            }

            List<Object> values = new ArrayList<>();
            json.elements().forEachRemaining(element -> {
                values.add(mapJsonToSchemaWithoutCheckTypes(schema.valueSchema(), element));
            });

            return values;
        }

        return this.schemaTransformer.transformJsonLiteral(json).value();
    }

    public Object mapJsonToSchemaCheckTypes(Schema schema, JsonNode json) {
        if (json == null || json.isNull()) {
            if (schema.isOptional()) {
                return null;
            }

            throw new IllegalArgumentException("Empty value for non optional field.");
        }

        if (schema.type().equals(Schema.Type.STRUCT)) {
            if (!json.isObject()) {
                throw new IllegalArgumentException("Expected an object to map to a structure.");
            }

            Struct struct = new Struct(schema);
            json.fields().forEachRemaining(field -> {
                JsonNode fieldValue = field.getValue();
                if (fieldValue == null || fieldValue.isNull()) {
                    return;
                }

                Field fieldInSchema = schema.field(field.getKey());
                if (fieldInSchema == null) {
                    throw new IllegalArgumentException("Field "+field.getKey()+" does not seem to exist here.");
                }

                struct.put(field.getKey(), mapJsonToSchemaCheckTypes(
                        fieldInSchema.schema(),
                        fieldValue
                ));
            });

            return struct;
        } else if (schema.type().equals(Schema.Type.ARRAY)) {
            if (!json.isArray()) {
                throw new IllegalArgumentException("Expected an array to map to an array.");
            }

            List<Object> values = new ArrayList<>();
            json.elements().forEachRemaining(element -> {
                values.add(mapJsonToSchemaCheckTypes(schema.valueSchema(), element));
            });

            return values;
        }

        SchemaAndValue literalSchemaAndValue = this.schemaTransformer.transformJsonLiteral(json);
        if (literalSchemaAndValue.schema().type() != schema.type())
            throw new DataException("Schemas in literals are different");
        return literalSchemaAndValue.value();
    }
}
