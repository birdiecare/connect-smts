package com.birdie.kafka.connect.utils;

import com.birdie.kafka.connect.smt.DebeziumJsonDeserializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TransformDebeziumBenchMarkValidatingSchemas extends DebeziumJsonDeserializer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransformDebeziumBenchMarkValidatingSchemas.class);
    private final SchemaMapperBenchMark schemaMapper = new SchemaMapperBenchMark(this.schemaTransformer);

    public TransformDebeziumBenchMarkValidatingSchemas() {
        super();
    }


    @Override
    public SchemaAndValue transformDebeziumJsonField(SourceRecord record, Field field, String jsonString) {
        JsonNode jsonNode;
        try {
            jsonNode = this.objectMapper.readTree(jsonString);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot parse JSON value \""+jsonString+"\"", e);
        }

        // Probabilistic optimisation: tries to map the message to one of the known schemas.
        if (unionPreviousMessagesSchema && useProbabilisticFastPath) {
            for (Schema schema: this.getOrCreateListOfKnownSchemasForField(record.topic(), field.name())) {
                try {
                    Object value = this.schemaMapper.mapJsonToSchemaWithoutCheckTypes(schema, jsonNode);
                    if (value == null) {
                        return null;
                    }

                    // Checking if previous schema is valid
                    try{
                        ConnectSchema.validateValue(schema, value);
                        return new SchemaAndValue(schema, value);
                    }
                    catch (DataException e) {
                        LOGGER.debug("Schema mapping with previous schemas failed: " + e);
                    }
                } catch (Exception e) {
                    // This opportunistic attempt failed, we will transform and merge the schemas.
                }
            }
        }

        SchemaAndValue transformed = schemaTransformer.transform(field, jsonNode);
        if (!unionPreviousMessagesSchema || transformed == null) {
            return transformed;
        }

        List<Schema> knownSchemas = this.getOrCreateListOfKnownSchemasForField(record.topic(), field.name());

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
                LOGGER.info("Updating schema "+field.name()+"#"+i+" on topic "+record.topic()+" with a unified schema ("+LoggingContext.createContext(record)+"): "+this.schemaSerDer.serialize(unionedSchema));

                knownSchemas.set(i, unionedSchema);
            }

            return new SchemaAndValue(
                    unionedSchema,
                    this.schemaTransformer.repackage(unionedSchema, transformed.value())
            );
        }

        // We couldn't unified with any known schema so far so we add this one to our stack.
        LOGGER.info("Registering schema "+field.name()+"#"+knownSchemas.size()+" on topic "+record.topic()+" for future unions ("+LoggingContext.createContext(record)+"): "+this.schemaSerDer.serialize(transformed.schema()));
        knownSchemas.add(transformed.schema());

        return transformed;
    }

}
