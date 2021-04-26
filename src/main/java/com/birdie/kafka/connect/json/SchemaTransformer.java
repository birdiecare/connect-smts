package com.birdie.kafka.connect.json;

import com.birdie.kafka.connect.utils.AvroUtils;
import com.birdie.kafka.connect.utils.StructWalker;
import org.apache.kafka.connect.data.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
            Schema childSchema = null;
            List<Object> transformedChildren = new ArrayList<>();

            for (final ListIterator<Object> it = ((JSONArray) obj).listIterator(); it.hasNext();) {
                final Object child = it.next();
                SchemaAndValue transformedChild = transformJsonValue(child, key+"_array_item");

                if (childSchema == null) {
                    childSchema = transformedChild.schema();
                } else if (childSchema.type() != transformedChild.schema().type()) {
                    throw new IllegalArgumentException("Child #"+it.previousIndex()+" as type '"+transformedChild.schema().type()+"' while previously gathered '"+childSchema.type()+"'.");
                } else if (childSchema.type().equals(Schema.Type.STRUCT)) {
                    childSchema = unionStruct(childSchema, transformedChild.schema());
                }

                transformedChildren.add(transformedChild.value());
            }

            // We need to re-create the `Struct` objects.
            if (childSchema != null && childSchema.type().equals(Schema.Type.STRUCT)) {
                List<Object> repackagedStructured = new ArrayList<>();

                for (Object transformedChild: transformedChildren) {
                    if (!(transformedChild instanceof Struct)) {
                        throw new IllegalArgumentException("A child for a structure has an invalid type: "+transformedChild.getClass().getName()+".");
                    }

                    Struct repackagedStructure = new Struct(childSchema);
                    for (Field field: ((Struct) transformedChild).schema().fields()) {
                        repackagedStructure.put(field.name(), ((Struct) transformedChild).get(field.name()));
                    }

                    repackagedStructured.add(repackagedStructure);
                }

                // Replace.
                transformedChildren = repackagedStructured;
            }

            // By default, if array is empty, it's an empty struct
            if (childSchema == null) {
                SchemaBuilder schemaBuilder = SchemaBuilder.struct();
                if (optionalStructFields) {
                    schemaBuilder.optional();
                }
                childSchema = schemaBuilder.name(key+"_array_item").build();
            }

            SchemaBuilder schemaBuilder = SchemaBuilder.array(childSchema);
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

    Schema unionStruct(Schema left, Schema right) {
        if (!left.type().equals(right.type())) {
            throw new IllegalArgumentException("We can only union too schemas of the same type together.");
        } else if (!left.type().equals(Schema.Type.STRUCT)) {
            throw new IllegalArgumentException("We can only union structs together.");
        }

        SchemaBuilder schemaBuilder = SchemaBuilder.struct()
                .name(left.name());

        List<Field>[] streams = new List[]{
            left.fields(), right.fields()
        };

        Map<String, List<Field>> fieldsByName =
                Stream.of(streams)
                .flatMap(Collection::stream)
                .collect(Collectors.groupingBy(Field::name));

        for (Map.Entry<String, List<Field>> entry: fieldsByName.entrySet()) {
            Schema firstSchema = entry.getValue().get(0).schema();
            SchemaBuilder unionedSchema = new SchemaBuilder(firstSchema.type());

            // We don't have this field everywhere, we need it to be optional.
            // Field will also be optional if optional-struct-fields is true.
            if (entry.getValue().size() != streams.length || optionalStructFields) {
                unionedSchema.optional();
            }

            schemaBuilder.field(entry.getKey(), unionedSchema.build());
        }

        return schemaBuilder.build();
    }
}
