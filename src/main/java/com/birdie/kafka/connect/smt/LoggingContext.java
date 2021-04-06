package com.birdie.kafka.connect.smt;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Map;

public class LoggingContext {
    static String createContext(ConnectRecord record) {
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
}
