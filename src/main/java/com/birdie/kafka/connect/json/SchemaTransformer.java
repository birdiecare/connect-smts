package com.birdie.kafka.connect.json;

import com.birdie.kafka.connect.utils.AvroUtils;
import com.birdie.kafka.connect.utils.LoggingContext;
import com.birdie.kafka.connect.utils.StructWalker;
import com.jsoniter.JsonIterator;
import com.jsoniter.ValueType;
import com.jsoniter.any.Any;
import com.jsoniter.spi.JsonException;
import com.jsoniter.spi.JsoniterSpi;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.*;

public class SchemaTransformer {
    private JSONParser parser = new JSONParser();

    private boolean optionalStructFields;
    private boolean convertNumbersToDouble;
    private boolean sanitizeFieldsName;

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

    public SchemaTransformer(boolean optionalStructFields, boolean convertNumbersToDouble, boolean sanitizeFieldsName) {
        this.optionalStructFields = optionalStructFields;
        this.convertNumbersToDouble = convertNumbersToDouble;
        this.sanitizeFieldsName = sanitizeFieldsName;
    }

    public SchemaAndValue transform(Field field, String jsonValue) {
        try {
            return transformJsonValue(
                JsonIterator.deserialize(jsonValue),
                field.name()
            );
        } catch (JsonException e) {
            throw new IllegalArgumentException("Cannot parse JSON value \""+jsonValue+"\"", e);
        } 
    }

    SchemaAndValue transformJsonValue(Any obj, String key) {
        if (obj.valueType().equals(ValueType.OBJECT)) {
            List<Map.Entry<String, Any>> listOfEntries = new ArrayList<>();
            for (Map.Entry<String, Any> entry : (Set<Map.Entry<String, Any>>) obj.asMap().entrySet()) {
                listOfEntries.add(
                    new AbstractMap.SimpleEntry<>(
                        this.sanitizeFieldsName ? AvroUtils.sanitizeColumnName(entry.getKey()) : entry.getKey(),
                        entry.getValue()
                    )
                );
            }

            return StructWalker.walk(
                    key,
                    listOfEntries,
                    entry -> entry.getKey(),
                    entry -> transformJsonValue(entry.getValue(), key+"_"+entry.getKey()),
                    optionalStructFields
            );
        } else if (obj.valueType().equals(ValueType.ARRAY)) {
            List<Any> list = obj.asList();

            // We can't guess the type of an array's content if it's empty so we ignore it.
            if (list.size() == 0) {
                return null;
            }

            List<Schema> transformedSchemas = new ArrayList<>();
            List<Object> transformedValues = new ArrayList<>();

            for (Any child : list) {
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

                transformedSchema = schemaBuilder.name(key+"_array_item").build();
            }

            SchemaBuilder schemaBuilder = SchemaBuilder.array(transformedSchema);
            if (optionalStructFields) {
                schemaBuilder.optional();
            }

            return new SchemaAndValue(
                schemaBuilder.name(key+"_array").build(),
                    transformedValues
            );
        } else if (obj.valueType().equals(ValueType.NULL)) {
            return null;
        }

        Object actualValue = obj.object();
        System.out.println("type="+obj.getClass().getName());
        System.out.println("actualValue="+actualValue.getClass().getName());
        Schema.Type objSchemaType = Values.inferSchema(actualValue).type();

        if (convertNumbersToDouble && isNumberType(objSchemaType)) {
            actualValue = Double.valueOf(actualValue.toString());
            objSchemaType = Schema.Type.FLOAT64;
        }

        SchemaBuilder schemaBuilder = new SchemaBuilder(objSchemaType);

        if (optionalStructFields) {
            schemaBuilder.optional();
        }

        return new SchemaAndValue(schemaBuilder.build(), actualValue);
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
}
