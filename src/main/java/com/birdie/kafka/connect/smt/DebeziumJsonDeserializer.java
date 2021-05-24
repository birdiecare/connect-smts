package com.birdie.kafka.connect.smt;

import com.birdie.kafka.connect.json.SchemaTransformer;
import com.birdie.kafka.connect.utils.LoggingContext;
import com.birdie.kafka.connect.utils.SchemaSerDer;
import com.birdie.kafka.connect.utils.StructWalker;
import com.fasterxml.jackson.core.JsonProcessingException;
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
    private SchemaSerDer schemaSerDer = new SchemaSerDer();

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

    private List<Schema> getOrCreateListOfKnownSchemasForField(String fieldName) {
        if (!this.knownMessageSchemasPerField.containsKey(fieldName)) {
            this.knownMessageSchemasPerField.put(fieldName, new CopyOnWriteArrayList<>());
        }

        return this.knownMessageSchemasPerField.get(fieldName);
    }

    private SchemaAndValue transformDebeziumJsonField(SourceRecord record, Field field, String jsonString) {
        SchemaAndValue transformed = schemaTransformer.transform(field, jsonString);

        if (!unionPreviousMessagesSchema || transformed == null) {
            return transformed;
        }

        List<Schema> knownSchemas = this.getOrCreateListOfKnownSchemasForField(field.name());

        // Go through the various known schemas that we can unify. There is a list of them
        // because it might be that some schemas are simply incompatible with each other.
        for (int i = 0; i < knownSchemas.size(); i++) {
            Schema knownSchema = knownSchemas.get(i);

            Schema unionedSchema;
            try {
                unionedSchema = this.schemaTransformer.unionSchemas(knownSchema, transformed.schema()).build();
            } catch (IllegalArgumentException e) {
                // Could not union the schema with one of the known message schemas, that's fine...
                if (unionPreviousMessagesSchemaLogUnionErrors) {
                    LOGGER.warn("Could not union schemas with in-memory schema #"+i+" ("+LoggingContext.createContext(record)+", known-schema="+LoggingContext.describeSchema(knownSchemas.get(i))+", given-schema="+LoggingContext.describeSchema(transformed.schema())+")", e);
                }

                continue;
            }

            // If it worked and it's more generic, let's re-use that more generic schema going forward!
            if (!unionedSchema.equals(knownSchema)) {
                LOGGER.info("Updating schema "+field.name()+"#"+i+" with a unified schema ("+LoggingContext.createContext(record)+"): "+this.schemaSerDer.serialize(unionedSchema));

                knownSchemas.set(i, unionedSchema);
            }

            return new SchemaAndValue(
                unionedSchema,
                this.schemaTransformer.repackage(unionedSchema, transformed.value())
            );
        }

        // We couldn't unified with any known schema so far so we add this one to our stack.
        LOGGER.info("Registering schema "+field.name()+"#"+knownSchemas.size()+" for future unions ("+LoggingContext.createContext(record)+"): "+this.schemaSerDer.serialize(transformed.schema()));
        knownSchemas.add(transformed.schema());

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
        if (unionPreviousMessagesSchema) {
            String fieldSchemaConfigurationPrefix = ConfigName.UNION_PREVIOUS_MESSAGES_SCHEMA+".field.";

            for (String key: props.keySet()) {
                if (key.startsWith(fieldSchemaConfigurationPrefix)) {
                    String fieldName = key.substring(fieldSchemaConfigurationPrefix.length());
                    List<Schema> knownSchemas = this.getOrCreateListOfKnownSchemasForField(fieldName);

                    try {
                        knownSchemas.addAll(
                            this.schemaSerDer.deserializeMany(
                                (String) props.get(key)
                            )
                        );
                    } catch (JsonProcessingException e) {
                        throw new IllegalArgumentException("Could not initialise the SMT's schema for field '"+fieldName+"'.", e);
                    }
                }
            }
        }

        unionPreviousMessagesSchemaLogUnionErrors = config.getBoolean(ConfigName.UNION_PREVIOUS_MESSAGES_SCHEMA_LOG_UNION_ERRORS);
        schemaTransformer = new SchemaTransformer(
                config.getBoolean(ConfigName.OPTIONAL_STRUCT_FIELDS),
                config.getBoolean(ConfigName.CONVERT_NUMBERS_TO_DOUBLE),
                config.getBoolean(ConfigName.SANITIZE_FIELDS_NAME)
        );
    }
}
