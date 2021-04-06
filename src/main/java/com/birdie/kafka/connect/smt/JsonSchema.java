package com.birdie.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonSchema implements Transformation<SourceRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonSchema.class);

    private interface ConfigName {
        String NO_SCHEMA_BEHAVIOUR = "no-schema-behaviour";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(JsonSchema.ConfigName.NO_SCHEMA_BEHAVIOUR, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                    "What to do if missing schema? Can be either \"pass\" or \"fail\".");

    public static final ConfigDef COLUMN_CONFIG_DEF = new ConfigDef()
            .define("schema", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                    "Where is the schema.");

    private SchemaTransformer schemaTransformer = new SchemaTransformer();
    private boolean failOnMissingSchema;
    private Map<String, SimpleConfig> configPerColumn;


    @Override
    public SourceRecord apply(SourceRecord record) {
        if (record.valueSchema() == null) {
            throw new IllegalArgumentException("Only applies on messages with schema ("+LoggingContext.createContext(record)+")");
        }

        Schema schema = record.valueSchema();
        if (schema.type() != Schema.Type.STRUCT) {
            throw new IllegalArgumentException("Only applies on messages with a struct schema, got "+schema.type()+" ("+LoggingContext.createContext(record)+")");
        }

        SchemaBuilder builder = new SchemaBuilder(schema.type());
        Struct value = (Struct) record.value();

        HashMap<String, Object> newValues = new HashMap<>();

        System.out.println("record:        schema.name() = "+schema.name());

        for (Field field : schema.fields()) {
            if (field.schema().type() == Schema.Type.STRING) {
                System.out.println("yay, found a string field: "+field.name()+"; doc="+field.schema().doc()+"; name="+field.schema().name());

                if ("io.debezium.data.Json".equals(field.schema().name())) {
                    System.out.println("yay, marked as JSON by debezium!");

                    String jsonString = (String) value.get(field.name());

                    if (!configPerColumn.containsKey(field.name())) {
                        if (failOnMissingSchema) {
                            throw new IllegalArgumentException("Could not find schema for field "+field.name()+". ("+LoggingContext.createContext(record)+")");
                        } else {
                            LOGGER.error("Could not find schema for field "+field.name()+", skipping as per configuration. ("+LoggingContext.createContext(record)+")");
                            continue;
                        }
                    }

                    String schemaUrl = configPerColumn.get(field.name()).getString("schema");
                    try {
                        SchemaAndValue transformed = schemaTransformer.transform(schemaUrl, jsonString);

                        builder.field(field.name(), transformed.schema());
                        newValues.put(field.name(), transformed.value());
                    } catch (URISyntaxException e) {
                        throw new IllegalArgumentException("Cannot transform schema for type "+field.name()+". ("+LoggingContext.createContext(record)+")", e);
                    }

                    continue;
                }
            }

            builder.field(field.name(), field.schema());
            newValues.put(field.name(), value.get(field.name()));
        }

        Schema newSchema = builder.build();
        Struct newStruct = new Struct(newSchema);
        for (String key : newValues.keySet()) {
            newStruct.put(key, newValues.get(key));
        }


        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                newSchema, newStruct,
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

        HashMap<String, Map<String, String>> configPropsPerColumn = new HashMap<>();
        for (String key : props.keySet()) {
            if (key.startsWith("columns.")) {
                int indexOfDot = key.lastIndexOf(".");
                String columnName = key.substring("columns.".length(), indexOfDot);
                String configName = key.substring(indexOfDot + 1);

                if (!configPropsPerColumn.containsKey(columnName)) {
                    configPropsPerColumn.put(columnName, new HashMap<>());
                }

                configPropsPerColumn.get(columnName).put(configName, (String) props.get(key));
            }
        }

        this.configPerColumn = new HashMap<>();
        for (String columnName : configPropsPerColumn.keySet()) {
            this.configPerColumn.put(columnName, new SimpleConfig(COLUMN_CONFIG_DEF, configPropsPerColumn.get(columnName)));
        }

        failOnMissingSchema = config.getString(ConfigName.NO_SCHEMA_BEHAVIOUR).equals("fail");
    }
}
