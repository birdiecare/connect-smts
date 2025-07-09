package com.birdie.kafka.connect.json;

import com.birdie.kafka.connect.utils.AvroUtils;
import com.birdie.kafka.connect.utils.LoggingContext;
import com.birdie.kafka.connect.utils.StructWalker;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

public class SchemaTransformer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaTransformer.class);

    private boolean optionalStructFields;
    private boolean convertNumbersToDouble;
    private boolean sanitizeFieldsName;
    private List<String> ignoredFields;

    private static final Set<Schema.Type> numberSchemaTypes = new HashSet<Schema.Type>(Arrays.asList(
        Schema.Type.INT8,
        Schema.Type.INT16,
        Schema.Type.INT32,
        Schema.Type.INT64,
        Schema.Type.FLOAT32
    ));
    private Boolean isNumberType(Schema.Type schemaType) {
        return numberSchemaTypes.contains(schemaType);
    }

    public SchemaTransformer(boolean optionalStructFields, boolean convertNumbersToDouble, boolean sanitizeFieldsName, List<String> ignoredFields) {
        this.optionalStructFields = optionalStructFields;
        this.convertNumbersToDouble = convertNumbersToDouble;
        this.sanitizeFieldsName = sanitizeFieldsName;
        this.ignoredFields = ignoredFields;
    }

    public SchemaAndValue transform(Field field, JsonNode node) {
        return transformJsonValue(
            node,
            field.name()
        );
    }

    SchemaAndValue transformJsonValue(JsonNode obj, String key) {
        if (this.ignoredFields.contains(key)) {
            return null;   
        } else if (obj.isObject()) {
            List<Map.Entry<String, JsonNode>> listOfEntries = new ArrayList<>();
            for (Iterator<String> it = obj.fieldNames(); it.hasNext(); ) {
                String fieldName = it.next();
                listOfEntries.add(
                        new AbstractMap.SimpleEntry<>(
                                this.sanitizeFieldsName ? AvroUtils.sanitizeColumnName(fieldName) : fieldName,
                                obj.path(fieldName)
                        )
                );
            }

            return StructWalker.walk(
                    key,
                    listOfEntries,
                    entry -> entry.getKey(),
                    entry -> transformJsonValue(entry.getValue(), key + "_" + entry.getKey()),
                    optionalStructFields
            );
        } else if (obj.isArray()) {
            List<Schema> transformedSchemas = new ArrayList<>();
            List<Object> transformedValues = new ArrayList<>();

            for (Iterator<JsonNode> it = obj.elements(); it.hasNext(); ) {
                JsonNode child = it.next();
                SchemaAndValue t = transformJsonValue(child, key + "_array_item");

                if (t == null) {
                    transformedValues.add(null);
                } else {
                    transformedValues.add(t.value());
                    transformedSchemas.add(t.schema());
                }
            }

            Schema transformedSchema = transformedSchemas.size() > 0 ? unionSchemas(transformedSchemas.toArray(Schema[]::new)).build() : null;

            // We need to re-create the `Struct` objects.
            if (transformedSchema != null && transformedSchema.type().equals(Schema.Type.STRUCT)) {
                transformedValues = repackageList(transformedSchema, transformedValues);
            }

            // By default, if array is empty, it's an empty struct
            if (transformedSchema == null) {
                SchemaBuilder schemaBuilder = SchemaBuilder.struct();
                if (optionalStructFields) {
                    schemaBuilder.optional();
                }

                transformedSchema = schemaBuilder.name(key + "_array_item").build();
            }

            SchemaBuilder schemaBuilder = SchemaBuilder.array(transformedSchema);
            if (optionalStructFields) {
                schemaBuilder.optional();
            }

            return new SchemaAndValue(
                    schemaBuilder.name(key + "_array").build(),
                    transformedValues
            );
        } else if (obj.isNull()) {
            SchemaBuilder builder = SchemaBuilder.string().optional();
            return new SchemaAndValue(builder.build(), null);
        }

        return transformJsonLiteral(obj);
    }

    public SchemaAndValue transformJsonLiteral(JsonNode obj) {
        Object value = valueFromLiteralJacksonTreeNode(obj);
        // Values.inferSchema returns null for BigInt values.
        Schema.Type objSchemaType = value instanceof BigInteger ? Schema.Type.FLOAT64 : Values.inferSchema(value).type();
        if (value instanceof BigInteger) {
            BigInteger bigIntValue = (BigInteger) value;
            value =  bigIntValue.doubleValue();
        }

        if (convertNumbersToDouble && isNumberType(objSchemaType)) {
            value = Double.valueOf(value.toString());
            objSchemaType = Schema.Type.FLOAT64;
        }

        SchemaBuilder schemaBuilder = new SchemaBuilder(objSchemaType);

        if (optionalStructFields) {
            schemaBuilder.optional();
        }

        return new SchemaAndValue(schemaBuilder.build(), value);
    }

    public Object repackage(Schema schema, Object value) {
        if (value == null) {
            return null;
        }

        if (schema.type().equals(Schema.Type.ARRAY)) {
            return repackageList(schema.valueSchema(), value);
        } else if (schema.type().equals(Schema.Type.STRUCT)) {
            if (!(value instanceof Struct)) {
                throw new IllegalArgumentException("Expected value of type STRUCT, got "+value.getClass().getName());
            }

            return repackageStructure(schema, (Struct) value);
        }

        return value;
    }

    private Struct repackageStructure(Schema transformedSchema, Struct transformedStruct) {
        Struct repackagedStructure = new Struct(transformedSchema);

        for (Field field: transformedStruct.schema().fields()) {
            Object value = transformedStruct.get(field.name());
            Schema expectedSchema = transformedSchema.field(field.name()).schema();

            try {
                repackagedStructure.put(field.name(), repackage(expectedSchema, value));
            } catch (DataException e) {
                Schema valueSchema = value instanceof Struct ? ((Struct) value).schema() : null;
                throw new IllegalArgumentException("Could not construct struct successfully for field "+field.name()+": expected schema "+LoggingContext.describeSchema(expectedSchema)+"; received schema="+LoggingContext.describeSchema(valueSchema), e);
            }
        }

        return repackagedStructure;
    }

    private List<Object> repackageList(Schema itemSchema, Object value) {
        if (!(value instanceof List)) {
            throw new IllegalArgumentException("Expected value of schema type ARRAY to be a List but got "+(value != null ? value.getClass().getName() : "NULL"));
        }

        if (!itemSchema.type().equals(Schema.Type.STRUCT)) {
            return (List<Object>) value;
        }

        List<Object> repackagedStructured = new ArrayList<>();
        for (Object child: (List<Object>) value) {
            if (child == null) {
                repackagedStructured.add(null);
            } else if (child instanceof Struct) {
                repackagedStructured.add(repackageStructure(itemSchema, (Struct) child));
            } else {
                throw new IllegalArgumentException("A child for a structure has an invalid type: " + child.getClass().getName() + ".");
            }
        }

        return repackagedStructured;
    }

    public SchemaBuilder unionSchemas(Schema ...schemas) {
        if (schemas.length == 0) {
            throw new IllegalArgumentException("We can't union-ize an empty list of schemas.");
        }

        Schema.Type type = schemas[0].type();

        // Array-specific values we care about
        Schema.Type valueType = null;
        Schema[] valueSchemas = new Schema[schemas.length];

        // Struct-specific values we care about
        Map<String, List<Schema>> fieldsByName = new HashMap<>();

        for (int i = 0; i < schemas.length; i++) {
            Schema schema = schemas[i];

            if (!schema.type().equals(type)) {
                throw new IllegalArgumentException("We can only union schemas of the same type together. Found: " + type + " and " + schema.type());
            }

            if (schema.type().equals(Schema.Type.ARRAY)) {
                if (valueType == null) {
                    valueType = schema.valueSchema().type();
                } else if (!valueType.equals(schema.valueSchema().type())) {
                    throw new IllegalArgumentException("We can only union array schemas of the same value type together. Found: " + valueType + " and "+ schema.valueSchema().type());
                }

                valueSchemas[i] = schema.valueSchema();
            } else if (schema.type().equals(Schema.Type.STRUCT)) {
                for (Field field : schema.fields()) {
                    if (!fieldsByName.containsKey(field.name())) {
                        fieldsByName.put(field.name(), new ArrayList<>());
                    }

                    fieldsByName.get(field.name()).add(
                        field.schema()
                    );
                }
            }
        }

        SchemaBuilder schemaBuilder;
        if (type.equals(Schema.Type.ARRAY)) {
            schemaBuilder = SchemaBuilder.array(
                unionSchemas(valueSchemas).build()
            );
        } else if (type.equals(Schema.Type.STRUCT)) {
            schemaBuilder = SchemaBuilder.struct();

            String[] fieldNames = fieldsByName.keySet().toArray(String[]::new);
            Arrays.sort(fieldNames);

            for (String fieldName: fieldNames) {
                List<Schema> fieldSchemas = fieldsByName.get(fieldName);

                SchemaBuilder unionedSchema = unionSchemas(fieldSchemas.toArray(Schema[]::new));
                if (fieldSchemas.size() != schemas.length || optionalStructFields) {
                    unionedSchema.optional();
                }

                schemaBuilder.field(fieldName, unionedSchema.build());
            }
        } else {
            schemaBuilder = new SchemaBuilder(schemas[0].type());
        }

        schemaBuilder = schemaBuilder.name(schemas[0].name());

        boolean shouldBeOptional = false;
        for (Schema schema: schemas) {
            if (schema.isOptional()) {
                shouldBeOptional = true;
                break;
            }
        }

        if (shouldBeOptional) {
            schemaBuilder = schemaBuilder.optional();
        }

        return schemaBuilder;
    }

    public static Object valueFromLiteralJacksonTreeNode(JsonNode node) {
        if (node.isBinary()) {
            try {
                return node.binaryValue();
            } catch (IOException e) {
                LOGGER.error("Could not get the binary value from a JSON node, returning NULL instead.", e);

                return null;
            }
        } else if (node.isBoolean()) {
            return node.booleanValue();
        } else if (node.isNumber()) {
            return node.numberValue();
        } else if (node.getNodeType() == JsonNodeType.STRING) {
            return node.textValue();
        }

        throw new IllegalArgumentException("Found JSON node of type '"+node.getNodeType()+"' but not supported.");
    }
}
