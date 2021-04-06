package com.birdie.kafka.connect.smt;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.parser.OpenAPIV3Parser;
import io.swagger.v3.oas.models.media.Schema;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public class SwaggerSchemaFetcher {
    public Schema fetch(String urlAsString) throws URISyntaxException {
        URI url = new URI(urlAsString);
        OpenAPI openAPI = new OpenAPIV3Parser().read(urlAsString);
        Map<String, Schema> schemas = openAPI.getComponents().getSchemas();

        return findSchema(schemas, url.getFragment().split("\\."));
    }

    Schema findSchema(Map<String, Schema> schemas, String[] path) {
        if (path.length == 0) {
            throw new IllegalArgumentException("Path is empty ("+path.toString()+").");
        }

        Schema schema = schemas.get(path[0]);
        if (schema == null) {
            throw new IllegalArgumentException("Schema '"+path[0]+"' was not found.");
        }

        if (path.length == 1) {
            return schema;
        }

        return findSchema(schema.getProperties(), java.util.Arrays.copyOfRange(path, 1, path.length));
    }
}
