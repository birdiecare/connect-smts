package com.birdie.kafka.connect.utils;

import com.birdie.kafka.connect.smt.DebeziumJsonDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;

import java.util.Map;

public class DebeziumJsonDeserializerValidatingTypes extends DebeziumJsonDeserializer {

    public DebeziumJsonDeserializerValidatingTypes() {
        super();
    }

    @Override
    public void configure(Map<String, ?> props) {
        super.configure(props);
        this.schemaMapper = new SchemaMapperBenchMark(this.schemaTransformer);
    }

    @Override
    protected SchemaAndValue checkSchema(Schema schema, Object value) {
        return new SchemaAndValue(schema, value);
    }
}
