package com.birdie.kafka.connect.smt;

import com.birdie.kafka.connect.json.SchemaTransformer;
import com.birdie.kafka.connect.utils.LoggingContext;
import com.birdie.kafka.connect.utils.StructWalker;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class DebeziumJsonDeserializer implements Transformation<SourceRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumJsonDeserializer.class);

    private interface ConfigName {
        String OPTIONAL_STRUCT_FIELDS = "optional-struct-fields";
        String CONVERT_NUMBERS_TO_DOUBLE = "convert-numbers-to-double";
        String SANITIZE_FIELDS_NAME = "sanitize.field.names";
        String UNION_PREVIOUS_MESSAGES_SCHEMA = "union-previous-messages-schema";
        String UNION_PREVIOUS_MESSAGES_SCHEMA_LOG_UNION_ERRORS = "union-previous-messages-schema.log-union-errors";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(ConfigName.OPTIONAL_STRUCT_FIELDS, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM, "When true, all struct fields are optional.")
        .define(ConfigName.CONVERT_NUMBERS_TO_DOUBLE, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM, "When true, integers in structs are converted to doubles.")
        .define(ConfigName.SANITIZE_FIELDS_NAME, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM, "When true, automatically sanitises the fields name so they are compatible with Avro.")
        .define(ConfigName.UNION_PREVIOUS_MESSAGES_SCHEMA, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM, "When true, merges the message's schema with the previous messages' schemas.")
        .define(ConfigName.UNION_PREVIOUS_MESSAGES_SCHEMA_LOG_UNION_ERRORS, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM, "When true, if two schemas can't be merged with one another, it will log an error instead of just considering it normal.")
    ;

    private SchemaTransformer schemaTransformer;

    private boolean unionPreviousMessagesSchema;
    private boolean unionPreviousMessagesSchemaLogUnionErrors;
    private Map<String, List<Schema>> knownMessageSchemasPerField = new ConcurrentHashMap<>();

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
                        return transformDebeziumJsonField(record, field, jsonString);
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

    private SchemaAndValue transformDebeziumJsonField(SourceRecord record, Field field, String jsonString) {
        SchemaAndValue transformed = schemaTransformer.transform(field, jsonString);

        if (!unionPreviousMessagesSchema) {
            return transformed;
        }

        if (!this.knownMessageSchemasPerField.containsKey(field.name())) {
            this.knownMessageSchemasPerField.put(field.name(), new CopyOnWriteArrayList<>());
        }

        List<Schema> knownSchemas = this.knownMessageSchemasPerField.get(field.name());

        // Go through the various known schemas that we can unify. There is a list of them
        // because it might be that some schemas are simply incompatible with each other.
        for (int i = 0; i < knownSchemas.size(); i++) {
            Schema unionedSchema;

            try {
                unionedSchema = this.schemaTransformer.unionSchemas(
                        knownSchemas.get(i),
                        transformed.schema()
                );
            } catch (IllegalArgumentException e) {
                // Could not union the schema with one of the known message schemas, that's fine...
                if (unionPreviousMessagesSchemaLogUnionErrors) {
                    LOGGER.warn("Could not union schemas with in-memory schema #"+i+" ("+LoggingContext.createContext(record)+", known-schema="+LoggingContext.describeSchema(knownSchemas.get(i))+", given-schema="+LoggingContext.describeSchema(transformed.schema())+")", e);
                }

                continue;
            }

            // If it worked, let's re-use that more generic schema going forward!
            knownSchemas.set(i, unionedSchema);

            return new SchemaAndValue(
                unionedSchema,
                this.schemaTransformer.repackage(unionedSchema, transformed.value())
            );
        }

        // We couldn't unified with any known schema so far so we add this one to our stack.
        knownSchemas.add(transformed.schema());
        LOGGER.info("Registering the newly created schema on the in-memory known schemas for future unions ("+LoggingContext.createContext(record)+")");

        return transformed;
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

        unionPreviousMessagesSchema = config.getBoolean(ConfigName.UNION_PREVIOUS_MESSAGES_SCHEMA);
        unionPreviousMessagesSchemaLogUnionErrors = config.getBoolean(ConfigName.UNION_PREVIOUS_MESSAGES_SCHEMA_LOG_UNION_ERRORS);
        schemaTransformer = new SchemaTransformer(
                config.getBoolean(ConfigName.OPTIONAL_STRUCT_FIELDS),
                config.getBoolean(ConfigName.CONVERT_NUMBERS_TO_DOUBLE),
                config.getBoolean(ConfigName.SANITIZE_FIELDS_NAME)
        );
    }
}
