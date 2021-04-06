package com.birdie.kafka.connect.smt;

import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

// Copied from private class `MirrorUtils`
// @see https://github.com/apache/kafka/blob/2.7/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorUtils.java#L52-L62
final class MirrorUtils {
    static Map<String, Object> wrapPartition(TopicPartition topicPartition, String sourceClusterAlias) {
        Map<String, Object> wrapped = new HashMap();
        wrapped.put("topic", topicPartition.topic());
        wrapped.put("partition", topicPartition.partition());
        wrapped.put("cluster", sourceClusterAlias);
        return wrapped;
    }

    static Map<String, Object> wrapOffset(long offset) {
        return Collections.singletonMap("offset", offset);
    }
}
