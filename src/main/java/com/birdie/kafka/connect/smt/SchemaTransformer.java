package com.birdie.kafka.connect.smt;

import io.swagger.v3.oas.models.media.ArraySchema;
import org.apache.kafka.connect.data.*;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.net.URISyntaxException;

public class SchemaTransformer {
    SwaggerSchemaFetcher fetcher = new SwaggerSchemaFetcher();

    public SchemaAndValue transform(String schemaUrl, String jsonValue) throws URISyntaxException {
        return Values.parseString(jsonValue);

        //        Schema schema = toConnectSchema(
//            fetcher.fetch(schemaUrl)
//        );
//
//        return new SchemaAndValue(
//            schema,
//            toConnectValue(jsonValue, schema)
//        );
    }

    Schema toConnectSchema(io.swagger.v3.oas.models.media.Schema swagger) {

        if (swagger instanceof ArraySchema) {
            ((ArraySchema) swagger).getItems();
        }

        throw new IllegalArgumentException("Type '"+swagger.getType()+"' not supported.");
    }

    Object toConnectValue(String jsonValue, Schema schema) {

        if (schema.type() == Schema.Type.ARRAY) {
            JSONArray jsonArray = new JSONArray(new JSONTokener(jsonValue));

            return jsonArray.toList();
        } else if (schema.type() == Schema.Type.STRUCT) {
            Struct struct = new Struct(schema);
            JSONObject jsonObject = new JSONObject(new JSONTokener(jsonValue));

            for (String key : jsonObject.keySet()) {
                struct.put(key, jsonObject.get(key));
            }

            return struct;
        }

        throw new IllegalArgumentException("Unknown type '"+schema.type()+"'.");
    }
}
