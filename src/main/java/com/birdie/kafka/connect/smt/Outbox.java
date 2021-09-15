package com.birdie.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;

public class Outbox implements Transformation<SourceRecord> {
    private interface ConfigName {
        String TOPIC = "topic";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(Outbox.ConfigName.TOPIC, ConfigDef.Type.STRING, ConfigDef.Importance.MEDIUM, "The name of the topic to send messages to.")
    ;

    private String targetTopic;

    @java.lang.Override
    public SourceRecord apply(SourceRecord sourceRecord) {
        Struct value = (Struct) sourceRecord.value();

        return sourceRecord.newRecord(
                targetTopic,
                value.getInt32("partition_number"),
                sourceRecord.keySchema(),
                sourceRecord.key(),
                sourceRecord.valueSchema().field("payload").schema(),
                value.get("payload"),
                sourceRecord.timestamp()
        );
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
