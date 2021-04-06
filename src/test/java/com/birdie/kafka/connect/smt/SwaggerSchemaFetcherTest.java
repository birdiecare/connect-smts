package com.birdie.kafka.connect.smt;

import io.swagger.v3.oas.models.media.Schema;
import org.junit.Test;

import java.net.URISyntaxException;

import static org.junit.Assert.*;

public class SwaggerSchemaFetcherTest {
    @Test
    public void testFetch() throws URISyntaxException {
        Schema schema = new SwaggerSchemaFetcher().fetch("https://visit-planning.production.birdie.care/docs.json#VisitInstance.tasks");

        assertNotNull(schema);
        assertEquals("array", schema.getType());
    }
}
