package com.birdie.kafka.connect.smt;

import org.apache.kafka.connect.data.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SchemaTransformer {
    private boolean optionalStructFields;

    public SchemaTransformer(boolean optionalStructFields) {
        this.optionalStructFields = optionalStructFields;
    }

    public SchemaAndValue transform(Field field, String jsonValue) throws ParseException {
        return transformJsonValue(
                new JSONParser().parse(jsonValue),
                field.name()
        );
    }

    SchemaAndValue transformJsonValue(Object obj, String key) {
        if (obj instanceof JSONObject) {
            JSONObject object = (JSONObject) obj;

            return StructWalker.walk(
                    key,
                    (Set<Map.Entry<String, Object>>) object.entrySet(),
                    entry -> entry.getKey(),
                    entry -> transformJsonValue(entry.getValue(), key+"_"+entry.getKey())
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
                childSchema = SchemaBuilder.struct().name(key+"_array_item").build();
            }

            return new SchemaAndValue(
                SchemaBuilder.array(childSchema).name(key+"_array").build(),
                transformedChildren
            );
        }

        SchemaBuilder schemaBuilder = new SchemaBuilder(Values.inferSchema(obj).type());

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

            schemaBuilder.field(entry.getKey(), unionedSchema);
        }

        return schemaBuilder.build();
    }
}
