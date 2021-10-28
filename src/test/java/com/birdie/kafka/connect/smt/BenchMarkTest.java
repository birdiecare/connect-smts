package com.birdie.kafka.connect.smt;

import com.birdie.kafka.connect.utils.DebeziumJsonDeserializerValidatingTypes;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.*;

public class BenchMarkTest extends DebeziumJsonDeserializerTest{

    @Test
    public void runBenchmarks() throws Exception {
        Options options = new OptionsBuilder()
                .include(this.getClass().getName() + ".*")
                .mode(Mode.AverageTime)
                .warmupTime(TimeValue.seconds(1))
                .warmupIterations(2)
                .threads(1)
                .measurementIterations(2)
                .forks(1)
                .shouldFailOnError(true)
                .shouldDoGC(true)
                .build();

        new Runner(options).run();
    }

    private void executeTestOneStruct(DebeziumJsonDeserializer deserializer) {
        Struct firstMessageContents = new Struct(simpleSchema);
        firstMessageContents.put("id", "1234-5678");
        firstMessageContents.put("json", "true");

        Struct secondMessageContents = new Struct(simpleSchema);
        secondMessageContents.put("id", "1234-5679");
        secondMessageContents.put("json", "500");

        Struct thirdMessageContents = new Struct(simpleSchema);
        thirdMessageContents.put("id", "1234-5680");
        thirdMessageContents.put("json", "{\n" +
                "    \"enabled\": true,\n" +
                "    \"generated_at\": \"2021-10-27T06:22:13.487Z\",\n" +
                "    \"qr_code_content\": \"2342dfs-32342-dsdf22\"\n" +
                "}");

        Struct fourthMessageContents = new Struct(simpleSchema);
        fourthMessageContents.put("id", "1234-5678");
        fourthMessageContents.put("json", "false");

        Struct fifthMessageContents = new Struct(simpleSchema);
        fifthMessageContents.put("id", "1234-5678");
        fifthMessageContents.put("json", "1000");

        deserializer.configure(new HashMap<>() {{
            put("optional-struct-fields", "true");
            put("union-previous-messages-schema", "true");
            put("probabilistic-fast-path", "true");
        }});

        SourceRecord firstRecord = deserializer.apply(sourceRecordFromValue(firstMessageContents));
        assertTrue(firstRecord.valueSchema().fields().stream().anyMatch(field -> field.schema().type().getName().equals("boolean")));
        SourceRecord secondRecord = deserializer.apply(sourceRecordFromValue(secondMessageContents));
        assertTrue(secondRecord.valueSchema().fields().stream().anyMatch(field -> field.schema().type().getName().equals("int32")));
        SourceRecord thirdRecord = deserializer.apply(sourceRecordFromValue(thirdMessageContents));
        assertTrue(thirdRecord.valueSchema().fields().stream().anyMatch(field -> field.schema().type().getName().equals("struct")));
        SourceRecord fourthRecord = deserializer.apply(sourceRecordFromValue(fourthMessageContents));
        assertTrue(fourthRecord.valueSchema().fields().stream().anyMatch(field -> field.schema().type().getName().equals("boolean")));
        SourceRecord fifthRecord = deserializer.apply(sourceRecordFromValue(fifthMessageContents));
        assertTrue(fifthRecord.valueSchema().fields().stream().anyMatch(field -> field.schema().type().getName().equals("int32")));

    }


    private void executeTestMultipleStruct(DebeziumJsonDeserializer deserializer) {
        Struct firstMessageContents = new Struct(simpleSchema);
        firstMessageContents.put("id", "1234-5678");
        firstMessageContents.put("json", "true");

        Struct secondMessageContents = new Struct(simpleSchema);
        secondMessageContents.put("id", "1234-5679");
        secondMessageContents.put("json", "500");

        Struct thirdMessageContents = new Struct(simpleSchema);
        thirdMessageContents.put("id", "1234-5680");
        thirdMessageContents.put("json", "{\n" +
                "    \"enabled\": true,\n" +
                "    \"generated_at\": \"2021-10-27T06:22:13.487Z\",\n" +
                "    \"qr_code_content\": \"2342dfs-32342-dsdf22\"\n" +
                "}");

        Struct fourthMessageContents = new Struct(simpleSchema);
        fourthMessageContents.put("id", "1234-5680");
        fourthMessageContents.put("json", "{\n" +
                "    \"enabled\": true,\n" +
                "    \"generated_at\": \"2021-10-27T06:22:13.487Z\",\n" +
                "    \"qr_code_content\": \"2342dfs-32342-dsdf22\"\n" +
                "}");

        Struct fifthMessageContents = new Struct(simpleSchema);
        fifthMessageContents.put("id", "1234-5681");
        fifthMessageContents.put("json", "{\n" +
                "    \"enabled\": true,\n" +
                "    \"generated_at\": \"2021-10-27T06:22:13.487Z\",\n" +
                "    \"qr_code_content\": \"2342dfs-32342-dsdf22\"\n" +
                "}");

        Struct sixthMessageContents = new Struct(simpleSchema);
        sixthMessageContents.put("id", "1234-5682");
        sixthMessageContents.put("json", "{\n" +
                "    \"enabled\": true,\n" +
                "    \"generated_at\": \"2021-10-27T06:22:13.487Z\",\n" +
                "    \"qr_code_content\": \"2342dfs-32342-dsdf22\"\n" +
                "}");

        Struct seventhMessageContents = new Struct(simpleSchema);
        seventhMessageContents.put("id", "1234-5683");
        seventhMessageContents.put("json", "2500");

        deserializer.configure(new HashMap<>() {{
            put("optional-struct-fields", "true");
            put("union-previous-messages-schema", "true");
            put("probabilistic-fast-path", "true");
        }});

        SourceRecord firstRecord = deserializer.apply(sourceRecordFromValue(firstMessageContents));
        assertTrue(firstRecord.valueSchema().fields().stream().anyMatch(field -> field.schema().type().getName().equals("boolean")));
        SourceRecord secondRecord = deserializer.apply(sourceRecordFromValue(secondMessageContents));
        assertTrue(secondRecord.valueSchema().fields().stream().anyMatch(field -> field.schema().type().getName().equals("int32")));
        SourceRecord thirdRecord = deserializer.apply(sourceRecordFromValue(thirdMessageContents));
        assertTrue(thirdRecord.valueSchema().fields().stream().anyMatch(field -> field.schema().type().getName().equals("struct")));
        SourceRecord fourthRecord = deserializer.apply(sourceRecordFromValue(fourthMessageContents));
        assertTrue(fourthRecord.valueSchema().fields().stream().anyMatch(field -> field.schema().type().getName().equals("struct")));
        SourceRecord fifthRecord = deserializer.apply(sourceRecordFromValue(fifthMessageContents));
        assertTrue(fifthRecord.valueSchema().fields().stream().anyMatch(field -> field.schema().type().getName().equals("struct")));
        SourceRecord sixthRecord = deserializer.apply(sourceRecordFromValue(sixthMessageContents));
        assertTrue(sixthRecord.valueSchema().fields().stream().anyMatch(field -> field.schema().type().getName().equals("struct")));
        SourceRecord seventhRecord = deserializer.apply(sourceRecordFromValue(seventhMessageContents));
        assertTrue(seventhRecord.valueSchema().fields().stream().anyMatch(field -> field.schema().type().getName().equals("int32")));

    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void benchMarkValidatingSchemasOneStruct() {
        executeTestOneStruct(new DebeziumJsonDeserializer());
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void benchMarkValidatingTypesOneStruct() {
        executeTestOneStruct(new DebeziumJsonDeserializerValidatingTypes());
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void benchMarkValidatingSchemasMultipleStruct() {
        executeTestMultipleStruct(new DebeziumJsonDeserializer());
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void benchMarkValidatingTypesMultipleStruct() {
        executeTestMultipleStruct(new DebeziumJsonDeserializerValidatingTypes());
    }
}
