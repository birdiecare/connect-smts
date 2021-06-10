package com.birdie.kafka.connect.json;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.ArrayList;
import java.util.List;

public class SchemaMapper {
    SchemaTransformer schemaTransformer;

    public SchemaMapper(SchemaTransformer schemaTransformer)
    {
        this.schemaTransformer = schemaTransformer;
    }

    public Object mapJsonToSchema(Schema schema, JsonNode json) {
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

                struct.put(field.getKey(), mapJsonToSchema(
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
                values.add(mapJsonToSchema(schema.valueSchema(), element));
            });

            return values;
        }

        return this.schemaTransformer.transformJsonLiteral(json).value();
    }
}
