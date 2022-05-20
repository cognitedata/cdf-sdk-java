package com.cognite.client.servicesV1.parser;

import com.cognite.client.dto.Transformation;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TransformationJobMetricsParser {

    static final ObjectMapper objectMapper = new ObjectMapper();

    public static Transformation.Job.Metric parseTransformationJobMetrics(String json) throws JsonProcessingException {
        JsonNode root = objectMapper.readTree(json);
        Transformation.Job.Metric.Builder builder = Transformation.Job.Metric.newBuilder();
        if (root.isObject()) {
            extractNodes(builder, root);
        }

        return builder.build();
    }

    public static void extractNodes(Transformation.Job.Metric.Builder builder, JsonNode root) {
        if (root.path("timestamp").isLong()) {
            builder.setTimestamp(root.path("timestamp").longValue());
        }
        if (root.path("name").isTextual()) {
            builder.setName(root.path("name").textValue());
        }
        if (root.path("count").isIntegralNumber()) {
            builder.setCount(root.path("count").intValue());
        }
    }
}
