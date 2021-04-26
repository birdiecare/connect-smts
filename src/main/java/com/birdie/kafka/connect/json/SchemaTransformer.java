package com.birdie.kafka.connect.json;

import com.birdie.kafka.connect.utils.StructWalker;
import org.apache.kafka.connect.data.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.*;
import java.util.stream.Collectors;

public class SchemaTransformer {
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
                new JSONParser().parse(jsonValue),
                field.name()
            );
        } catch (ParseException e) {
            throw new IllegalArgumentException("Cannot parse JSON value \""+jsonValue+"\"", e);
        } 
    }

    SchemaAndValue transformJsonValue(Object obj, String key) {
        if (obj instanceof JSONObject) {
            JSONObject object = (JSONObject) obj;

            return StructWalker.walk(
                    key,
                    (Set<Map.Entry<String, Object>>) object.entrySet(),
                    entry -> entry.getKey(),
                    entry -> transformJsonValue(entry.getValue(), key+"_"+entry.getKey()),
                    optionalStructFields,
                    sanitizeFieldsName
            );
        } else if (obj instanceof JSONArray) {
            List<SchemaAndValue> transformed = (List<SchemaAndValue>) ((JSONArray) obj).stream().map(
                    child -> transformJsonValue(child, key+"_array_item")
            ).collect(Collectors.toList());

            Schema[] transformedSchemas = transformed.stream().map(SchemaAndValue::schema).toArray(Schema[]::new);
            Schema transformedSchema = transformedSchemas.length > 0 ? unionSchemas(transformedSchemas).build() : null;

            List<Object> transformedChildren = transformed.stream().map(SchemaAndValue::value).collect(Collectors.toList());

            // We need to re-create the `Struct` objects.
            if (transformedSchema != null && transformedSchema.type().equals(Schema.Type.STRUCT)) {
                List<Object> repackagedStructured = new ArrayList<>();

                for (SchemaAndValue transformedChild: transformed) {
                    if (!(transformedChild.value() instanceof Struct)) {
                        throw new IllegalArgumentException("A child for a structure has an invalid type: "+transformedChild.value().getClass().getName()+".");
                    }

                    Struct transformedStruct = (Struct) transformedChild.value();
                    Struct repackagedStructure = new Struct(transformedSchema);

                    for (Field field: transformedStruct.schema().fields()) {
                        repackagedStructure.put(field.name(), transformedStruct.get(field.name()));
                    }

                    repackagedStructured.add(repackagedStructure);
                }

                // Replace.
                transformedChildren = repackagedStructured;
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
                transformedChildren
            );

        } else if (obj == null) {
            return null;
        }
        
        Schema.Type objSchemaType = Values.inferSchema(obj).type();

        if (convertNumbersToDouble && isNumberType(objSchemaType)) {
            obj = Double.valueOf(obj.toString());
            objSchemaType = Schema.Type.FLOAT64;
        }

        SchemaBuilder schemaBuilder = new SchemaBuilder(objSchemaType);

        if (optionalStructFields) {
            schemaBuilder.optional();
        }

        return new SchemaAndValue(schemaBuilder.build(), obj);
    }

    SchemaBuilder unionSchemas(Schema ...schemas) {
        if (schemas.length == 0) {
            throw new IllegalArgumentException("We can't union-ize an empty list of schemas.");
        }

        List<String> types = List.of(schemas)
                .stream()
                .map(schema -> schema.type().toString())
                .distinct()
                .collect(Collectors.toList());

        if (types.size() != 1) {
            throw new IllegalArgumentException("We can only union schemas of the same type together. Found: " + String.join(",", types));
        }

        Schema.Type type = Schema.Type.valueOf(types.get(0));

        if (type.equals(Schema.Type.ARRAY)) {
            List<String> valueTypes = List.of(schemas)
                    .stream()
                    .map(schema -> schema.valueSchema().type().toString())
                    .distinct()
                    .collect(Collectors.toList());

            if (valueTypes.size() != 1) {
                throw new IllegalArgumentException("We can only union array schemas of the same value type together. Found: " + String.join(",", types));
            }

            return SchemaBuilder.array(
                schemas[0].valueSchema()
            ).name(schemas[0].name());
        } else if (type.equals(Schema.Type.STRUCT)) {
            SchemaBuilder schemaBuilder = SchemaBuilder.struct()
                    .name(schemas[0].name());

            Map<String, List<Field>> fieldsByName =
                    List.of(schemas)
                            .stream()
                            .map(Schema::fields)
                            .flatMap(Collection::stream)
                            .collect(Collectors.groupingBy(Field::name));

            for (Map.Entry<String, List<Field>> entry : fieldsByName.entrySet()) {
                List<Field> fields = entry.getValue();

                SchemaBuilder unionedSchema = unionSchemas(
                    fields.stream().map(Field::schema).toArray(Schema[]::new)
                );

                if (fields.size() != schemas.length || optionalStructFields) {
                    unionedSchema.optional();
                }

                schemaBuilder.field(entry.getKey(), unionedSchema.build());
            }

            return schemaBuilder;
        }

        return new SchemaBuilder(schemas[0].type())
                .name(schemas[0].name());
    }
}
