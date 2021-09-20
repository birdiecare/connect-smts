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
        String DEBUG_LOG_LEVEL = "debug-log-level";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(Outbox.ConfigName.TOPIC, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, "The name of the topic to send messages to.")
        .define(Outbox.ConfigName.PARTITION_SETTING, ConfigDef.Type.STRING, "partition-number", ConfigDef.Importance.MEDIUM, "Set to \"partition-number\" (default) to use the record's partition_number value, or set to \"partition-key\" to generate partition number using record's partition_key value")
        .define(Outbox.ConfigName.NUMBER_OF_PARTITION_IN_TOPIC, ConfigDef.Type.INT, 0, ConfigDef.Importance.MEDIUM, "Number of partitions on the target topic")
        .define(Outbox.ConfigName.DEBUG_LOG_LEVEL, ConfigDef.Type.STRING, "trace", ConfigDef.Importance.LOW, "Log level for debugging purposes")
    ;

    private String targetTopic;
    private PartitionSetting partitionSetting;
    private Integer numberOfPartitionsInTargetTopic;
    private DebugLogLevel debugLogLevel;

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
            throw new IllegalArgumentException("num-target-partitions is zero/null, when auto-partitioning is set to true");
        }
        
        String debugLogLevelString = config.getString(ConfigName.DEBUG_LOG_LEVEL);
        try {
            debugLogLevel = DebugLogLevel.valueOf(debugLogLevelString.toUpperCase());
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid log level provided: " + debugLogLevelString, e);
        }
    }

    @java.lang.Override
    public SourceRecord apply(SourceRecord sourceRecord) {
        DEBUG("Received source record: {}", sourceRecord);

        if (sourceRecord.value() == null) {
            DEBUG("Dropping debezium-generated tombstones with null partition_key: {}", sourceRecord);
            return null;
        }

        Schema valueSchema = sourceRecord.valueSchema();
        if (valueSchema.name().equals("io.debezium.connector.common.Heartbeat")) {
            DEBUG("Ignoring debezium-connector heartbeat: {}", sourceRecord);
            return sourceRecord;
        }

        Struct value = (Struct) sourceRecord.value();

        Schema transformedSchema;
        Object transformedValue;
        if (valueSchema.field(DELETED_FIELD) != null && value.getString(DELETED_FIELD).equals("true")) {
            DEBUG("Generating tombstone from debezium-deletion message: {}", sourceRecord);
            transformedSchema = null;
            transformedValue = null;
        } else {
            transformedSchema = sourceRecord.valueSchema().field("payload").schema();
            transformedValue = value.get("payload");
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

        SourceRecord transformedRecord = sourceRecord.newRecord(
            topic,
            getPartitionNumber(sourceRecord),
            sourceRecord.keySchema(),
            sourceRecord.key(),
            transformedSchema,
            transformedValue,
            sourceRecord.timestamp(),
            getHeaders(sourceRecord)
        );

        DEBUG("Emitting transformed record: {}", transformedRecord);
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
            DEBUG("Header field does not exist on sourceRecord {}", sourceRecord);
        } else if (value.get(HEADERS_FIELD) == null) {
            DEBUG("Header field is null on sourceRecord {}", sourceRecord);
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

    private Integer getPartitionNumber(SourceRecord sourceRecord) {
        switch (partitionSetting) {
            case PARTITION_NUMBER:
                return getExplicitPartitionNumber(sourceRecord);
            case PARTITION_KEY:
                return getGeneratedPartitionNumber(sourceRecord);
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
    
    private Integer getGeneratedPartitionNumber(SourceRecord sourceRecord) {
        String partitionKey;
        try {
            partitionKey = ((Struct) sourceRecord.value()).getString(PARTITION_KEY_FIELD);
            assert partitionKey != null : "partition_key is null";
        } catch (Exception|AssertionError e) {
            throw new DataException("Unable to find partition_key in source record " + sourceRecord.toString(), e);
        }
        try {
            return Utils.toPositive(Utils.murmur2(partitionKey.getBytes())) % numberOfPartitionsInTargetTopic;
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

    // Logging levels
    public enum DebugLogLevel {
        DEBUG, TRACE, INFO
    }

    private void DEBUG(String format, Object ...args) {
        switch (this.debugLogLevel) {
            case DEBUG:
                LOGGER.debug(format, args);
                break;
            case TRACE:
                LOGGER.trace(format, args);
                break;
            case INFO:
                LOGGER.info(format, args);
                break;
            default:
                throw new IllegalArgumentException("Invalid log level provided");
        }
    }
    
}
