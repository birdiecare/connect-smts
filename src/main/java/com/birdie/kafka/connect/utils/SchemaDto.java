package com.birdie.kafka.connect.utils;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.ArrayList;
import java.util.List;

public class SchemaDto {
    public Schema.Type type;
    public String name;
    public boolean isOptional;
    public SchemaDto valueSchema;
    public List<FieldDto> fields;

    public static SchemaDto fromSchema(Schema schema) {
        SchemaDto dto = new SchemaDto();
        dto.name = schema.name();
        dto.type = schema.type();
        dto.isOptional = schema.isOptional();

        if (schema.type().equals(Schema.Type.ARRAY)) {
            dto.valueSchema = SchemaDto.fromSchema(schema.valueSchema());
        } else if (schema.type().equals(Schema.Type.STRUCT)) {
            dto.fields = new ArrayList<>();
            for (Field field: schema.fields()) {
                dto.fields.add(FieldDto.fromField(field));
            }
        }

        return dto;
    }

    public Schema toSchema() {
        SchemaBuilder builder;
        if (this.type.equals(Schema.Type.STRUCT)) {
            builder = SchemaBuilder.struct();

            for (FieldDto field: this.fields) {
                builder.field(field.name, field.schema.toSchema());
            }
        } else if (this.type.equals(Schema.Type.ARRAY)) {
            builder = SchemaBuilder.array(this.valueSchema.toSchema());
        } else {
            builder = SchemaBuilder.type(this.type);
        }

        if (this.isOptional) {
            builder = builder.optional();
        }

        return builder.name(this.name).build();
    }

    public static class FieldDto {
        public String name;
        public int index;
        public SchemaDto schema;

        public static FieldDto fromField(Field field) {
            FieldDto dto = new FieldDto();
            dto.name = field.name();
            dto.index = field.index();
            dto.schema = SchemaDto.fromSchema(field.schema());

            return dto;
        }
    }
}
