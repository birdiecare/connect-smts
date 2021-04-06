package com.birdie.kafka.connect.smt.json;

import org.apache.kafka.common.config.ConfigException;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class DefaultSchemaDownloader implements SchemaDownloader {
    @Override
    public Schema downloadSchema(URL schemaUrl) {
        return loadSchema(loadObject(schemaUrl));
    }

    public static JSONObject loadObject(URL schemaUrl) {
        try {
            try (InputStream inputStream = schemaUrl.openStream()) {
                return loadObject(inputStream);
            }
        } catch (IOException e) {
            ConfigException exception = new ConfigException("url", schemaUrl, "error downloading schema");
            exception.initCause(e);
            throw exception;
        }
    }

    public static JSONObject loadObject(InputStream inputStream) {
        return new JSONObject(new JSONTokener(inputStream));
    }

    public static org.everit.json.schema.Schema loadSchema(JSONObject rawSchema) {
        return SchemaLoader.builder()
                .draftV7Support()
//                .addFormatValidator(new DateFormatValidator())
//                .addFormatValidator(new TimeFormatValidator())
//                .addFormatValidator(new DateTimeFormatValidator())
//                .addFormatValidator(new DecimalFormatValidator())
//                .addFormatValidator(new CustomTimestampFormatValidator())
                .schemaJson(rawSchema)
                .build()
                .load()
                .build();
    }
}
