package com.birdie.kafka.connect.smt;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.Map;

public class Outbox implements Transformation<SourceRecord> {
    public static final String DELETED_FIELD = "__deleted";
    public static final String PARTITION_KEY_FIELD = "partition_key";
    public static final String HEADERS_FIELD = "headers";
    public static final String TOPIC_FIELD = "topic";

    private static final Logger LOGGER = LoggerFactory.getLogger(Outbox.class);

    private ObjectMapper objectMapper = new ObjectMapper();
    private interface ConfigName {
        String TOPIC = "topic";
        String PARTITION_SETTING = "partition-setting";
        String NUMBER_OF_PARTITION_IN_TOPIC = "num-partitions";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(Outbox.ConfigName.TOPIC, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, "The name of the topic to send messages to.")
        .define(Outbox.ConfigName.PARTITION_SETTING, ConfigDef.Type.STRING, "partition-number", ConfigDef.Importance.MEDIUM, "Set to \"partition-number\" (default) to use the record's partition_number value, or set to \"partition-key\" to generate partition number using record's partition_key value")
        .define(Outbox.ConfigName.NUMBER_OF_PARTITION_IN_TOPIC, ConfigDef.Type.INT, 0, ConfigDef.Importance.MEDIUM, "Number of partitions on the target topic")
    ;

    private String targetTopic;
    private PartitionSetting partitionSetting;
    private Integer numberOfPartitionsInTargetTopic;

    public enum PartitionSetting {
        PARTITION_KEY, PARTITION_NUMBER
    }

    @java.lang.Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);

        targetTopic = config.getString(ConfigName.TOPIC);
        numberOfPartitionsInTargetTopic = config.getInt(ConfigName.NUMBER_OF_PARTITION_IN_TOPIC);

        String partitionSettingString = config.getString(ConfigName.PARTITION_SETTING);
        try {
            partitionSetting = PartitionSetting.valueOf(partitionSettingString.toUpperCase().replace('-', '_'));
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid partition setting provided: " + partitionSettingString, e);
        }
        if (partitionSetting == PartitionSetting.PARTITION_KEY && numberOfPartitionsInTargetTopic == 0) {
            // throw new IllegalArgumentException("num-target-partitions is zero/null, when auto-partitioning is set to true");
        }
    }

    @java.lang.Override
    public SourceRecord apply(SourceRecord sourceRecord) {
        LOGGER.debug("Received source record: {}", sourceRecord);

        if (sourceRecord.value() == null) {
            LOGGER.debug("Dropping debezium-generated tombstones with null partition_key: {}", sourceRecord);
            return null;
        }

        Schema valueSchema = sourceRecord.valueSchema();
        if (valueSchema.name().equals("io.debezium.connector.common.Heartbeat")) {
            LOGGER.debug("Ignoring debezium-connector heartbeat: {}", sourceRecord);
            return sourceRecord;
        }

        Struct value = (Struct) sourceRecord.value();

        Schema transformedSchema;
        Object transformedValue;
        if (valueSchema.field(DELETED_FIELD) != null && value.getString(DELETED_FIELD).equals("true")) {
            LOGGER.debug("Generating tombstone from debezium-deletion message: {}", sourceRecord);
            transformedSchema = null;
            transformedValue = null;
        } else {
            transformedValue = value.get("payload");
            transformedSchema = transformedValue != null ? sourceRecord.valueSchema().field("payload").schema() : null;
        }

        String topic = targetTopic;
        if (valueSchema.field(TOPIC_FIELD) != null) {
            Object targetTopicObject = value.get(TOPIC_FIELD);
            if (targetTopicObject != null) {
                if (!(targetTopicObject instanceof String)) {
                    throw new DataException("Target topic provided should be a string, got "+targetTopicObject.getClass().getName());
                }

                topic = (String) targetTopicObject;
            }
        } else if (topic == null) {
            throw new DataException("Target topic wasn't provided in the source table nor the configuration.");
        }

        TopicDescription topicAndPartitionNumber = TopicDescription.fromString(topic);

        SourceRecord transformedRecord = sourceRecord.newRecord(
            topicAndPartitionNumber.topic,
            getPartitionNumber(topicAndPartitionNumber, sourceRecord),
            sourceRecord.keySchema(),
            sourceRecord.key(),
            transformedSchema,
            transformedValue,
            sourceRecord.timestamp(),
            getHeaders(sourceRecord)
        );

        LOGGER.debug("Emitting transformed record: {}", transformedRecord);
        return transformedRecord;
    }

    private Headers getHeaders(SourceRecord sourceRecord) {
        Headers headers = sourceRecord.headers();
        Schema valueSchema = sourceRecord.valueSchema();
        Struct value = (Struct) sourceRecord.value();

        if (partitionSetting.equals(PartitionSetting.PARTITION_KEY)) {
            headers.add(
                PARTITION_KEY_FIELD, 
                ((Struct) sourceRecord.value()).getString(PARTITION_KEY_FIELD),
                Schema.STRING_SCHEMA
            );
        }

        Field headerField = valueSchema.field(HEADERS_FIELD);

        if (headerField == null) {
            LOGGER.debug("Header field does not exist on sourceRecord {}", sourceRecord);
        } else if (value.get(HEADERS_FIELD) == null) {
            LOGGER.debug("Header field is null on sourceRecord {}", sourceRecord);
        } else {
            Schema.Type schemaType = headerField.schema().type();
            switch (schemaType) {
                case STRUCT:
                    value.getStruct(HEADERS_FIELD).schema().fields().forEach(field -> {
                        headers.add(
                            field.name(),
                            value.getStruct(HEADERS_FIELD).getString(field.name()),
                            Schema.STRING_SCHEMA
                        );
                    });
                    break;
                case STRING:
                    try {
                        objectMapper.readValue(
                            value.getString(HEADERS_FIELD),
                            new TypeReference<HashMap<String, String>>() {}
                        ).forEach((k, v) -> {
                            headers.add(k, v, Schema.STRING_SCHEMA);
                        });
                    } catch (JsonProcessingException e) {
                        LOGGER.error("Could not decode headers.", e);
                    }
                    break;
                default:
                    LOGGER.error("Field 'headers' should be org.apache.kafka.connect.data.Schema.Type.STRUCT, was {}", schemaType);
                    break;
            }
        }

        return headers;
    }

    private Integer getPartitionNumber(TopicDescription topic, SourceRecord sourceRecord) {
        switch (partitionSetting) {
            case PARTITION_NUMBER:
                return getExplicitPartitionNumber(sourceRecord);
            case PARTITION_KEY:
                return getGeneratedPartitionNumber(topic, sourceRecord);
            default:
                throw new IllegalArgumentException("Invalid partitionSetting provided " + partitionSetting);
        }
    }

    private Integer getExplicitPartitionNumber(SourceRecord sourceRecord) {
        try {
            Integer partition = ((Struct) sourceRecord.value()).getInt32("partition_number");
            assert partition != null : "partition_number is null";
            return partition;
        } catch (Exception|AssertionError e) {
            throw new DataException("Unable to find partition_number in source record " + sourceRecord.toString(), e);
        }
    }
    
    private Integer getGeneratedPartitionNumber(TopicDescription topic, SourceRecord sourceRecord) {
        String partitionKey;
        try {
            partitionKey = ((Struct) sourceRecord.value()).getString(PARTITION_KEY_FIELD);
            assert partitionKey != null : "partition_key is null";
        } catch (Exception|AssertionError e) {
            throw new DataException("Unable to find partition_key in source record " + sourceRecord.toString(), e);
        }
        try {
            Integer numberOfPartitions = topic.numberOfPartitions;
            if (numberOfPartitions == null) {
                numberOfPartitions = this.numberOfPartitionsInTargetTopic;

                if (numberOfPartitions == null) {
                    throw new DataException("Unable to find the number of partitions for this target topic.");
                }
            }

            return Utils.toPositive(Utils.murmur2(partitionKey.getBytes())) % numberOfPartitions;
        } catch (Exception e) {
            throw new DataException("Unable to generate partition_number from partition_key " + partitionKey, e);
        }
    }

    @java.lang.Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @java.lang.Override
    public void close() {
        // not in use
    }
    static class TopicDescription {
        public String topic;
        public Integer numberOfPartitions;

        private TopicDescription(String topic, Integer partitionNumber) {
            this.topic = topic;
            this.numberOfPartitions = partitionNumber;
        }

        public static TopicDescription fromString(String string) {
            String[] parts = string.split("@");

            if (parts.length > 2) {
                throw new IllegalArgumentException("Topic name '"+string+"' is invalid.");
            } else if (parts.length == 2) {
                return new TopicDescription(parts[0], Integer.valueOf(parts[1]));
            }

            return new TopicDescription(string, null);
        }
    }
}
