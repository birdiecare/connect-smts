package com.birdie.kafka.connect.utils;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.Collection;
import java.util.HashMap;
import java.util.function.Function;

public class StructWalker {
    public static <T> SchemaAndValue walk(
            String name,
            Collection<T> items,
            Function<T, String> identifierFn,
            Function<T, SchemaAndValue> transformerFn
    ) {
        SchemaBuilder builder = SchemaBuilder.struct().name(name);
        HashMap<String, Object> valuesPerField = new HashMap<>();

        for (T item: items) {
            String identifier = identifierFn.apply(item);
            SchemaAndValue field = transformerFn.apply(item);

            if (field != null) {
                builder.field(identifier, field.schema());
                valuesPerField.put(identifier, field.value());
            }
        }

        Schema newSchema = builder.build();
        Struct newStruct = new Struct(newSchema);

        for (String key : valuesPerField.keySet()) {
            newStruct.put(key, valuesPerField.get(key));
        }

        return new SchemaAndValue(newSchema, newStruct);
    }
}
