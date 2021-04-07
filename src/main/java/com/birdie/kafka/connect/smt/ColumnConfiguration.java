package com.birdie.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;

public class ColumnConfiguration {
    public interface ConfigName {
        String SCHEMA = "schema";
    }

    public static final ConfigDef DEFINITION = new ConfigDef()
            .define(ConfigName.SCHEMA, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Where is the schema.");
}
