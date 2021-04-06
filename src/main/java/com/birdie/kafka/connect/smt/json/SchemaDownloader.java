package com.birdie.kafka.connect.smt.json;

import org.everit.json.schema.Schema;

import java.net.URL;

public interface SchemaDownloader {
    Schema downloadSchema(URL schemaUrl);
}
