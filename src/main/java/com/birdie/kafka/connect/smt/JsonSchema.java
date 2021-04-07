package com.birdie.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.json.simple.parser.ParseException;
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
//        String NO_SCHEMA_BEHAVIOUR = "no-schema-behaviour";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef();
//            .define(JsonSchema.ConfigName.NO_SCHEMA_BEHAVIOUR, ConfigDef.Type.STRING, "pass", ConfigDef.Importance.MEDIUM,
//                    "What to do if missing schema? Can be either \"pass\" or \"fail\".");

    private SchemaTransformer schemaTransformer = new SchemaTransformer();
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
                    if (jsonString == null) {
                        return null;
                    }

                    try {
                        return schemaTransformer.transform(field, jsonString);
                    } catch (URISyntaxException | ParseException e) {
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
            this.configPerColumn.put(columnName, new SimpleConfig(ColumnConfiguration.DEFINITION, configPropsPerColumn.get(columnName)));
        }
    }
}
