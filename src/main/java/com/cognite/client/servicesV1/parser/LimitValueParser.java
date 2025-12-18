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

import com.cognite.client.dto.LimitValue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.cognite.client.servicesV1.ConnectorConstants.MAX_LOG_ELEMENT_LENGTH;

/**
 * This class contains a set of methods to help parsing limit value objects between Cognite API representations
 * (json and proto) and typed objects.
 */
public class LimitValueParser {
    static final String logPrefix = "LimitValueParser - ";
    static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Parses a limit value json string to {@code LimitValue} proto object.
     *
     * @param json The JSON string to parse
     * @return The parsed LimitValue object
     * @throws Exception if parsing fails
     */
    public static LimitValue parseLimitValue(String json) throws Exception {
        JsonNode root = objectMapper.readTree(json);
        LimitValue.Builder builder = LimitValue.newBuilder();

        // limitId is required
        if (root.path("limitId").isTextual()) {
            builder.setLimitId(root.get("limitId").textValue());
        } else {
            throw new Exception(logPrefix + "Unable to parse attribute: limitId. Item excerpt: "
                    + json.substring(0, Math.min(json.length() - 1, MAX_LOG_ELEMENT_LENGTH)));
        }

        // value is required
        if (root.path("value").isIntegralNumber()) {
            builder.setValue(root.get("value").longValue());
        } else {
            throw new Exception(logPrefix + "Unable to parse attribute: value. Item excerpt: "
                    + json.substring(0, Math.min(json.length() - 1, MAX_LOG_ELEMENT_LENGTH)));
        }

        return builder.build();
    }

    /**
     * Builds a request item object from {@link LimitValue}.
     *
     * @param element The LimitValue to convert
     * @return A map representing the request body
     */
    public static Map<String, Object> toRequestInsertItem(LimitValue element) {
        return ImmutableMap.<String, Object>builder()
                .put("limitId", element.getLimitId())
                .put("value", element.getValue())
                .build();
    }
}

