package com.cognite.client.servicesV1.response;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class JsonStringAttributeResponseParserTest {
    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    @Tag("remoteCDP")
    void parseNestedPath() throws Exception {
        String jsonObject = new StringBuilder()
                .append("{\n")
                .append("\"error\": {\n")
                .append("\"message\": \"Files not uploaded:\"\n")
                .append("}}")
                .toString();

        LOG.info("Json object: {}", jsonObject);

        JsonStringAttributeResponseParser parser = JsonStringAttributeResponseParser.create()
                .withAttributePath("error.message");

        List<String> parsedStrings = parser.extractItems(jsonObject);
        LOG.info("Parsing results: {}", parsedStrings);

        assertEquals(parsedStrings.get(0), "Files not uploaded:");
    }
}