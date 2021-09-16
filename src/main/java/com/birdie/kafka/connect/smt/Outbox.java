package com.birdie.kafka.connect.smt;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class Outbox implements Transformation<SourceRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Outbox.class);

    private interface ConfigName {
        String TOPIC = "topic";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(Outbox.ConfigName.TOPIC, ConfigDef.Type.STRING, ConfigDef.Importance.MEDIUM, "The name of the topic to send messages to.")
    ;

    private String targetTopic;
    private ObjectMapper objectMapper = new ObjectMapper();

    @java.lang.Override
    public SourceRecord apply(SourceRecord sourceRecord) {
        Struct value = (Struct) sourceRecord.value();

        Schema valueSchema = sourceRecord.valueSchema();
        Field headerField = valueSchema.field("headers");

        SourceRecord transformed = sourceRecord.newRecord(
                targetTopic,
                value.getInt32("partition_number"),
                sourceRecord.keySchema(),
                sourceRecord.key(),
                sourceRecord.valueSchema().field("payload").schema(),
                value.get("payload"),
                sourceRecord.timestamp()
        );

        if (headerField != null) {
            if (!headerField.schema().type().equals(Schema.Type.STRING)) {
                LOGGER.error("Field 'headers' should be a string.");
            } else {
                Headers headers = sourceRecord.headers();

                try {
                    HashMap<String, String> headersToBeAdded = objectMapper.readValue(
                            value.getString("headers"),
                            new TypeReference<HashMap<String, String>>() {}
                    );

                    for (String key : headersToBeAdded.keySet()) {
                        headers.add(key, headersToBeAdded.get(key), Schema.STRING_SCHEMA);
                    }

                    transformed = sourceRecord.newRecord(
                            sourceRecord.topic(),
                            sourceRecord.kafkaPartition(),
                            sourceRecord.keySchema(),
                            sourceRecord.key(),
                            sourceRecord.valueSchema(),
                            sourceRecord.value(),
                            sourceRecord.timestamp(),
                            headers
                    );
                } catch (JsonProcessingException e) {
                    LOGGER.error("Could not decode headers.", e);
                }
            }
        }

        return transformed;
    }


    @java.lang.Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);

        targetTopic = config.getString(ConfigName.TOPIC);
    }

    @java.lang.Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @java.lang.Override
    public void close() {
    }
}
