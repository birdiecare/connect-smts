package com.birdie.kafka.connect.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Schema;

public class SchemaSerDer {
    private ObjectMapper objectMapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

    public String serialize(Schema schema) {
        if (schema == null) {
            return "[null]";
        }

        try {
            return this.objectMapper.writeValueAsString(
                SchemaDto.fromSchema(schema)
            );
        } catch (JsonProcessingException e) {
            return "[ERROR: "+e.getMessage()+"]";
        }
    }

    public Schema deserialize(String string) throws JsonProcessingException {
        SchemaDto schema = this.objectMapper.readValue(string, SchemaDto.class);

        return schema.toSchema();
    }
}
