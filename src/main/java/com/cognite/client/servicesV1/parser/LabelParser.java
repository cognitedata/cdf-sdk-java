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

import com.cognite.client.dto.Label;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.cognite.client.servicesV1.ConnectorConstants.MAX_LOG_ELEMENT_LENGTH;

/**
 * This class contains a set of methods to help parsing file objects between Cognite api representations
 * (json and proto) and typed objects.
 */
public class LabelParser {
    static final String logPrefix = "LabelParser - ";
    static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Parses an event json string to <code>Label</code> proto object.
     *
     * @param json
     * @return
     * @throws Exception
     */
    public static Label parseLabel(String json) throws Exception {
        JsonNode root = objectMapper.readTree(json);
        Label.Builder labelBuilder = Label.newBuilder();

        // An asset must contain an externalId and name.
        if (root.path("externalId").isTextual()) {
            labelBuilder.setExternalId(root.get("externalId").textValue());
        } else {
            throw new Exception(logPrefix + "Unable to parse attribute: externalId. Item exerpt: "
                    + json
                    .substring(0, Math.min(json.length() - 1, MAX_LOG_ELEMENT_LENGTH)));
        }

        if (root.path("name").isTextual()) {
            labelBuilder.setName(root.get("name").textValue());
        } else {
            throw new Exception(logPrefix + "Unable to parse attribute: name");
        }

        // The rest of the attributes are optional.
        if (root.path("description").isTextual()) {
            labelBuilder.setDescription(root.get("description").textValue());
        }

        if (root.path("createdTime").isIntegralNumber()) {
            labelBuilder.setCreatedTime(root.get("createdTime").longValue());
        }

        if (root.path("dataSetId").isIntegralNumber()) {
            labelBuilder.setDataSetId(root.get("dataSetId").longValue());
        }


        return labelBuilder.build();
    }

    /**
     * Builds a request insert item object from {@link Label}.
     *
     * An insert item object creates a new asset data object in the Cognite system.
     *
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestInsertItem(Label element) {
        // Note that "id" cannot be a part of an insert request.
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.<String, Object>builder()
                .put("externalId", element.getExternalId())
                .put("name", element.getName());

        if (element.hasDescription()) {
            mapBuilder.put("description", element.getDescription());
        }

        if (element.hasDataSetId()) {
            mapBuilder.put("dataSetId", element.getDataSetId());
        }

        return mapBuilder.build();
    }
}
