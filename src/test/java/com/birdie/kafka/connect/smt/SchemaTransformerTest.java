package com.birdie.kafka.connect.smt;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.Test;

import java.net.URISyntaxException;
import java.util.List;

import static org.junit.Assert.*;

public class SchemaTransformerTest {
    @Test
    public void testSchemaTransformer() throws URISyntaxException {
        SchemaAndValue transformed = new SchemaTransformer().transform(
                "https://visit-planning.production.birdie.care/docs.json#VisitInstance.tasks",
                "[\n" +
                        "  {\"type\": \"care_task\", \"id\": \"48385242-96d5-11eb-b8f1-4fc97a48a234\", \"note\": \"My note\", \"task_definition_id\": \"1234\"},\n" +
                        "  {\"type\": \"regular_task\", \"id\": \"502951c2-96d5-11eb-8776-33a3a6b06ce7\", \"external_schedule_id\": \"123\", \"time_of_day\": \"MORNING\", \"execution_offset\": 3600}\n" +
                        "]"
        );

        assertTrue(transformed.value() instanceof List);
    }
}
