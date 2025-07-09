package com.birdie.kafka.connect.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.List;

public class SchemaSerDer {
    private final ObjectMapper objectMapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.ALWAYS);

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

    public Schema deserializeOne(String string) throws JsonProcessingException {
        return this.objectMapper.readValue(string, SchemaDto.class).toSchema();
    }

    public List<Schema> deserializeMany(String string) throws JsonProcessingException {
        List<Schema> schemas = new ArrayList<>();
        for (SchemaDto dto: this.objectMapper.readValue(string, SchemaDto[].class)) {
            schemas.add(dto.toSchema());
        }

        return schemas;
    }
}
