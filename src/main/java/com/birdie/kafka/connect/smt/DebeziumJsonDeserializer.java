package com.birdie.kafka.connect.smt;

import com.birdie.kafka.connect.json.SchemaTransformer;
import com.birdie.kafka.connect.utils.LoggingContext;
import com.birdie.kafka.connect.utils.StructWalker;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.json.simple.parser.ParseException;

import java.util.Map;

public class DebeziumJsonDeserializer implements Transformation<SourceRecord> {
    private interface ConfigName {
        String OPTIONAL_STRUCT_FIELDS = "optional-struct-fields";
        String CONVERT_NUMBERS_TO_DOUBLE = "convert-numbers-to-double";
        String SANITIZE_FIELDS_NAME = "sanitize.field.names";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.OPTIONAL_STRUCT_FIELDS, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM, "When true, all struct fields are optional.")
            .define(ConfigName.CONVERT_NUMBERS_TO_DOUBLE, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM, "When true, integers in structs are converted to doubles.")
            .define(ConfigName.SANITIZE_FIELDS_NAME, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM, "When true, automatically sanitises the fields name so they are compatible with Avro.");

    private SchemaTransformer schemaTransformer;

    @Override
    public SourceRecord apply(SourceRecord record) {
        // Ignores tombstones
        if (record.value() == null) {
            return record;
        }

        if (record.valueSchema() == null) {
            throw new IllegalArgumentException("Only applies on messages with schema ("+ LoggingContext.createContext(record)+")");
        }

        Schema schema = record.valueSchema();
        if (schema.type() != Schema.Type.STRUCT) {
            throw new IllegalArgumentException("Only applies on messages with a struct schema, got "+schema.type()+" ("+LoggingContext.createContext(record)+")");
        }

        Struct value = (Struct) record.value();

        SchemaAndValue transformed = StructWalker.walk(
                schema.name(),
                schema.fields(),
                field -> field.name(),
                field -> {
                    if (field.schema().type() != Schema.Type.STRING
                            || !"io.debezium.data.Json".equals(field.schema().name())) {
                        return new SchemaAndValue(field.schema(), value.get(field.name()));
                    }

                    String jsonString = (String) value.get(field.name());
                    if (jsonString == null || "".equals(jsonString)) {
                        return null;
                    }

                    try {
                        return schemaTransformer.transform(field, jsonString);
                    } catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException("Cannot transform schema for type "+field.name()+". ("+LoggingContext.createContext(record)+")", e);
                    }
                }
        );

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                transformed.schema(),
                transformed.value(),
                record.timestamp()
        );
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        // Nothing to do...
    }

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);

        schemaTransformer = new SchemaTransformer(
                config.getBoolean(ConfigName.OPTIONAL_STRUCT_FIELDS),
                config.getBoolean(ConfigName.CONVERT_NUMBERS_TO_DOUBLE),
                config.getBoolean(ConfigName.SANITIZE_FIELDS_NAME)
        );
    }
}
