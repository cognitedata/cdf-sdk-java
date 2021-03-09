/*
 * Copyright (c) 2020 Cognite AS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cognite.client.servicesV1.parser;

import com.cognite.client.dto.Aggregate;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.StringValue;

import static com.cognite.client.servicesV1.ConnectorConstants.MAX_LOG_ELEMENT_LENGTH;

/**
 * This class contains a set of methods to help parsing file objects between Cognite api representations
 * (json and proto) and typed objects.
 */
public class AggregateParser {
    static final String logPrefix = "AggregateParser - ";
    static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Parses an aggregate json string to {@link Aggregate} proto object.
     *
     * @param json
     * @return
     * @throws Exception
     */
    public static Aggregate parseAggregate(String json) throws Exception {
        String jsonExcerpt = json.substring(0, Math.min(json.length() - 1, MAX_LOG_ELEMENT_LENGTH));
        JsonNode root = objectMapper.readTree(json);
        Aggregate.Builder aggregateBuilder = Aggregate.newBuilder();

        // must contain an items attribute
        if (root.path("items").isArray()) {
            for (JsonNode node : root.path("items")) {
                Aggregate.Record.Builder recordBuilder = Aggregate.Record.newBuilder();

                // must have a count
                if (node.path("count").isIntegralNumber()) {
                    recordBuilder.setCount(node.path("count").longValue());
                } else {
                    throw new Exception(logPrefix + "Unable to parse attribute: count. Json excerpt: " + jsonExcerpt);
                }

                if (node.path("value").isTextual()) {
                    recordBuilder.setValue(StringValue.of(node.path("value").textValue()));
                }

                aggregateBuilder.addAggregates(recordBuilder);
            }
        } else {
            throw new Exception(logPrefix + "Unable to parse attribute: items. Json excerpt: " + jsonExcerpt);
        }

        return aggregateBuilder.build();
    }
}
