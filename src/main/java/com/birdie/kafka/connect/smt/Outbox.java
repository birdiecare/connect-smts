package com.birdie.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class Outbox implements Transformation<SourceRecord> {
    private ObjectMapper objectMapper = new ObjectMapper();
    private interface ConfigName {
        String TOPIC = "topic";
        String AUTO_PARTITIONING = "auto-partitioning";
        String NUM_TARGET_PARTITIONS = "num-target-partitions";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(Outbox.ConfigName.TOPIC, ConfigDef.Type.STRING, ConfigDef.Importance.MEDIUM, "The name of the topic to send messages to.")
        .define(Outbox.ConfigName.AUTO_PARTITIONING, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM, "When true, SMT will generate new partition number using partition key (requires `num-target-partitions` config field and `partition_key` record field to be set).")
        .define(Outbox.ConfigName.NUM_TARGET_PARTITIONS, ConfigDef.Type.INT, 0, ConfigDef.Importance.MEDIUM, "Number of partitions on the target topic")
    ;

    private String targetTopic;
    private Boolean autoPartitioning;
    private Integer targetPartitions;

    @java.lang.Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);

        targetTopic = config.getString(ConfigName.TOPIC);
        autoPartitioning = config.getBoolean(ConfigName.AUTO_PARTITIONING);
        targetPartitions = config.getInt(ConfigName.NUM_TARGET_PARTITIONS);

        if (autoPartitioning == true && targetPartitions == 0) {
            throw new IllegalArgumentException("num-target-partitions is zero/null, when auto-partitioning is set to true");
        }
    }

    @java.lang.Override
    public SourceRecord apply(SourceRecord sourceRecord) {
        Struct value = (Struct) sourceRecord.value();

        return sourceRecord.newRecord(
                targetTopic,
                getPartitionNumber(sourceRecord),
                sourceRecord.keySchema(),
                sourceRecord.key(),
                sourceRecord.valueSchema().field("payload").schema(),
                value.get("payload"),
                sourceRecord.timestamp()
        );
    }

    private Integer getPartitionNumber(SourceRecord sourceRecord) {
        if (autoPartitioning == false) {
            return getExplicitPartitionNumber(sourceRecord);
        }
        return getGeneratedPartitionNumber(sourceRecord);
    }

    
    private Integer getExplicitPartitionNumber(SourceRecord sourceRecord) {
        Integer partition = ((Struct) sourceRecord.value()).getInt32("partition_number");

        if (partition == null) {
            throw new IllegalArgumentException("unable to find partition_number in source record");
        }

        return partition;
    }
    
    private Integer getGeneratedPartitionNumber(SourceRecord sourceRecord) {
        String partitionKey = ((Struct) sourceRecord.value()).getString("partition_key");
        
        if (partitionKey == null) {
            throw new IllegalArgumentException("partition_key not set in source record");
        }

        String payloadString = ((Struct) sourceRecord.value()).getString("payload");

        ObjectNode node;
        try {
            node = this.objectMapper.readValue(payloadString, ObjectNode.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("error reading partition_key in the source record");
        }

        // Support composite (comma-separated) partition keys
        String partitionKeyValue = Arrays.stream(partitionKey.split(","))
            .map(key -> {
                if (node.has(key)) {
                    return node.get(key).asText();
                }
                throw new IllegalArgumentException("no partition_key found in the source record");
            })
            .collect(Collectors.joining());

        Integer partition = Utils.toPositive(Utils.murmur2(partitionKeyValue.getBytes())) % targetPartitions;

        return partition;
    }

    @java.lang.Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @java.lang.Override
    public void close() {
    }
}
