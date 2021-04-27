package com.birdie.kafka.connect.utils;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Map;

public class LoggingContext {
    public static String createContext(ConnectRecord record) {
        String readableKey = record.key().toString();
        if (record.key() instanceof byte[]) {
            readableKey = new String((byte[]) record.key());
        }

        String context = "topic="+record.topic()+", partition="+record.kafkaPartition()+", key="+readableKey;
        if (record instanceof SourceRecord) {
            Map<String, ?> sourceOffset = ((SourceRecord) record).sourceOffset();
            if (sourceOffset != null) {
                Object offset = sourceOffset.get("offset");

                context += ", offset=" + (offset == null ? "{null}" : offset.toString());
            }
        }

        return context;
    }

    public static String describeSchema(Schema schema) {
        if (schema == null) {
            return null;
        }

        return schema.toString()+" (#"+schema.hashCode()+") optional="+schema.isOptional()+" version="+schema.version()+" type="+schema.type()+" defaultValue="+schema.defaultValue()
                + (schema.type().equals(Schema.Type.STRUCT) ? " fields="+schema.fields() : "");
    }
}
