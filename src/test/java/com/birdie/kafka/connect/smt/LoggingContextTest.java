package com.birdie.kafka.connect.smt;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LoggingContextTest {

    @Test()
    public void itDisplaysTheTopicPartitionAndKey() {
        final SourceRecord record = new SourceRecord(null, null, "test", 0, SchemaBuilder.bytes().optional().build(), "my-key".getBytes(), SchemaBuilder.bytes().optional().build(), new String("foo").getBytes());

        assertEquals("topic=test, partition=0, key=my-key", LoggingContext.createContext(record));
    }

    @Test()
    public void itDisplaysTheSourceOffset() {
        final SourceRecord record = new SourceRecord(
                MirrorUtils.wrapPartition(new TopicPartition("my-topic", 0), "cluster-1"),
                MirrorUtils.wrapOffset(123194),
                "my-topic", 0, SchemaBuilder.bytes().optional().build(), "my-key".getBytes(), SchemaBuilder.bytes().optional().build(), new String("foo").getBytes());

        assertEquals("topic=my-topic, partition=0, key=my-key, offset=123194", LoggingContext.createContext(record));
    }
}
