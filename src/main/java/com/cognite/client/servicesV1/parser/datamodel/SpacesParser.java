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

package com.cognite.client.servicesV1.parser.datamodel;

import com.cognite.client.dto.datamodel.Space;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.cognite.client.servicesV1.ConnectorConstants.MAX_LOG_ELEMENT_LENGTH;

/**
 * This class contains a set of methods to help parsing data model spaces objects between Cognite api representations
 * (json and proto) and typed objects.
 */
public class SpacesParser {
    static final String logPrefix = "SpacesParser - ";
    static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Parses a spaces json string to {@link Space} proto object.
     *
     * @param json
     * @return
     * @throws Exception
     */
    public static Space parseSpace(String json) throws Exception {
        JsonNode root = objectMapper.readTree(json);
        Space.Builder spaceBuilder = Space.newBuilder();

        // An event must contain an id.
        if (root.path("space").isTextual()) {
            spaceBuilder.setSpace(root.get("space").textValue());
        } else {
            String message = logPrefix + "Unable to parse attribute: space. Item excerpt: "
                    + json.substring(0, Math.min(json.length() - 1, MAX_LOG_ELEMENT_LENGTH));
            throw new Exception(message);
        }

        // The rest of the attributes are optional.
        if (root.path("name").isTextual()) {
            spaceBuilder.setDescription(root.get("name").textValue());
        }
        if (root.path("description").isTextual()) {
            spaceBuilder.setDescription(root.get("description").textValue());
        }
        if (root.path("createdTime").isIntegralNumber()) {
            spaceBuilder.setCreatedTime(root.get("createdTime").longValue());
        }
        if (root.path("lastUpdatedTime").isIntegralNumber()) {
            spaceBuilder.setLastUpdatedTime(root.get("lastUpdatedTime").longValue());
        }

       return spaceBuilder.build();
    }

    /**
     * Builds a request Upsert item object from {@link Space}.
     *
     * @param element
     * @return
     * @throws Exception
     */
    public static Map<String, Object> toRequestUpsertItem(Space element) {
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();

        mapBuilder.put("space", element.getSpace());

        // Optional attributes
        if (element.hasName()) {
            mapBuilder.put("name", element.getName());
        }
        if (element.hasDescription()) {
            mapBuilder.put("description", element.getDescription());
        }

        return mapBuilder.build();
    }
}
