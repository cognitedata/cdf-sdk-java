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

import com.cognite.client.dto.Event;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Iterator;
import java.util.Map;

import static com.cognite.client.servicesV1.ConnectorConstants.MAX_LOG_ELEMENT_LENGTH;

/**
 * This class contains a set of methods to help parsing file objects between Cognite api representations
 * (json and proto) and typed objects.
 */
public class EventParser {
    static final String logPrefix = "EventParser - ";
    static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Parses an event json string to <code>Event</code> proto object.
     *
     * @param json
     * @return
     * @throws Exception
     */
    public static Event parseEvent(String json) throws Exception {
        JsonNode root = objectMapper.readTree(json);
        Event.Builder eventBuilder = Event.newBuilder();

        // An event must contain an id.
        if (root.path("id").isIntegralNumber()) {
            eventBuilder.setId(root.get("id").longValue());
        } else {
            String message = logPrefix + "Unable to parse attribute: id. Item excerpt: "
                    + json.substring(0, Math.min(json.length() - 1, MAX_LOG_ELEMENT_LENGTH));
            throw new Exception(message);
        }

        // The rest of the attributes are optional.
        if (root.path("externalId").isTextual()) {
            eventBuilder.setExternalId(root.get("externalId").textValue());
        }
        if (root.path("startTime").isIntegralNumber()) {
            eventBuilder.setStartTime(root.get("startTime").longValue());
        }
        if (root.path("endTime").isIntegralNumber()) {
            eventBuilder.setEndTime(root.get("endTime").longValue());
        }
        if (root.path("description").isTextual()) {
            eventBuilder.setDescription(root.get("description").textValue());
        }
        if (root.path("type").isTextual()) {
            eventBuilder.setType(root.get("type").textValue());
        }
        if (root.path("subtype").isTextual()) {
            eventBuilder.setSubtype(root.get("subtype").textValue());
        }
        if (root.path("source").isTextual()) {
            eventBuilder.setSource(root.get("source").textValue());
        }
        if (root.path("createdTime").isIntegralNumber()) {
            eventBuilder.setCreatedTime(root.get("createdTime").longValue());
        }
        if (root.path("lastUpdatedTime").isIntegralNumber()) {
            eventBuilder.setLastUpdatedTime(root.get("lastUpdatedTime").longValue());
        }
        if (root.path("dataSetId").isIntegralNumber()) {
            eventBuilder.setDataSetId(root.get("dataSetId").longValue());
        }

        if (root.path("assetIds").isArray()) {
            for (JsonNode node : root.path("assetIds")) {
                if (node.isIntegralNumber()) {
                    eventBuilder.addAssetIds(node.longValue());
                }
            }
        }

        if (root.path("metadata").isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fieldIterator = root
                    .path("metadata").fields();
            while (fieldIterator.hasNext()) {
                Map.Entry<String, JsonNode> entry = fieldIterator.next();
                if (entry.getValue().isTextual()) {
                    eventBuilder
                            .putMetadata(entry.getKey(), entry.getValue().textValue());
                }
            }
        }

       return eventBuilder.build();
    }

    /**
     * Builds a request insert item object from <code>Event</code>.
     *
     * An insert item object creates a new Event data object in the Cognite system.
     *
     * @param element
     * @return
     * @throws Exception
     */
    public static Map<String, Object> toRequestInsertItem(Event element) throws Exception {
        // Note that "id" cannot be a part of an insert request.
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();

        if (element.hasExternalId()) {
            mapBuilder.put("externalId", element.getExternalId());
        }

        if (element.hasStartTime()) {
            mapBuilder.put("startTime", element.getStartTime());
        }
        if (element.hasEndTime()) {
            mapBuilder.put("endTime", element.getEndTime());
        }
        if (element.hasDescription()) {
            mapBuilder.put("description", element.getDescription());
        }
        if (element.hasType()) {
            mapBuilder.put("type", element.getType());
        }
        if (element.hasSubtype()) {
            mapBuilder.put("subtype", element.getSubtype());
        }
        if (element.getAssetIdsCount() > 0) {
            mapBuilder.put("assetIds", element.getAssetIdsList());
        }
        if (element.getMetadataCount() > 0) {
            mapBuilder.put("metadata", element.getMetadataMap());
        }
        if (element.hasSource()) {
            mapBuilder.put("source", element.getSource());
        }
        if (element.hasDataSetId()) {
            mapBuilder.put("dataSetId", element.getDataSetId());
        }

        return mapBuilder.build();
    }

    /**
     * Builds a request update item object from <code>Event</code>.
     *
     * An update item object updates an existing event object with new values for all provided fields.
     * Fields that are not in the update object retain their original value.
     *
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestUpdateItem(Event element) {
        Preconditions.checkArgument(element.hasExternalId() || element.hasId(),
                "Element must have externalId or Id in order to be written as an update");

        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, Object> updateNodeBuilder = ImmutableMap.builder();
        if (element.hasExternalId()) {
            mapBuilder.put("externalId", element.getExternalId());
        } else {
            mapBuilder.put("id", element.getId());
        }

        if (element.hasStartTime()) {
            updateNodeBuilder.put("startTime", ImmutableMap.of("set", element.getStartTime()));
        }
        if (element.hasEndTime()) {
            updateNodeBuilder.put("endTime", ImmutableMap.of("set", element.getEndTime()));
        }
        if (element.hasDescription()) {
            updateNodeBuilder.put("description", ImmutableMap.of("set", element.getDescription()));
        }
        if (element.hasType()) {
            updateNodeBuilder.put("type", ImmutableMap.of("set", element.getType()));
        }
        if (element.hasSubtype()) {
            updateNodeBuilder.put("subtype", ImmutableMap.of("set", element.getSubtype()));
        }
        if (element.getAssetIdsCount() > 0) {
            updateNodeBuilder.put("assetIds", ImmutableMap.of("set", element.getAssetIdsList()));
        }
        if (element.getMetadataCount() > 0) {
            updateNodeBuilder.put("metadata", ImmutableMap.of("add", element.getMetadataMap()));
        }
        if (element.hasSource()) {
            updateNodeBuilder.put("source", ImmutableMap.of("set", element.getSource()));
        }
        if (element.hasDataSetId()) {
            updateNodeBuilder.put("dataSetId", ImmutableMap.of("set", element.getDataSetId()));
        }
        mapBuilder.put("update", updateNodeBuilder.build());
        return mapBuilder.build();
    }

    /**
     * Builds a request update replace item object from <code>Event</code>.
     *
     * A replace item object replaces an existing event object with new values for all provided fields.
     * Fields that are not in the update object are set to null.
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestReplaceItem(Event element) {
        Preconditions.checkArgument(element.hasExternalId() || element.hasId(),
                "Element must have externalId or Id in order to be written as an update");

        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, Object> updateNodeBuilder = ImmutableMap.builder();
        if (element.hasExternalId()) {
            mapBuilder.put("externalId", element.getExternalId());
        } else {
            mapBuilder.put("id", element.getId());
        }

        if (element.hasStartTime()) {
            updateNodeBuilder.put("startTime", ImmutableMap.of("set", element.getStartTime()));
        } else {
            updateNodeBuilder.put("startTime", ImmutableMap.of("setNull", true));
        }

        if (element.hasEndTime()) {
            updateNodeBuilder.put("endTime", ImmutableMap.of("set", element.getEndTime()));
        } else {
            updateNodeBuilder.put("endTime", ImmutableMap.of("setNull", true));
        }

        if (element.hasDescription()) {
            updateNodeBuilder.put("description", ImmutableMap.of("set", element.getDescription()));
        } else {
            updateNodeBuilder.put("description", ImmutableMap.of("setNull", true));
        }

        if (element.hasType()) {
            updateNodeBuilder.put("type", ImmutableMap.of("set", element.getType()));
        } else {
            updateNodeBuilder.put("type", ImmutableMap.of("setNull", true));
        }

        if (element.hasSubtype()) {
            updateNodeBuilder.put("subtype", ImmutableMap.of("set", element.getSubtype()));
        } else {
            updateNodeBuilder.put("subtype", ImmutableMap.of("setNull", true));
        }

        if (element.getAssetIdsCount() > 0) {
            updateNodeBuilder.put("assetIds", ImmutableMap.of("set", element.getAssetIdsList()));
        } else {
            updateNodeBuilder.put("assetIds", ImmutableMap.of("set", ImmutableList.<Long>of()));
        }

        if (element.getMetadataCount() > 0) {
            updateNodeBuilder.put("metadata", ImmutableMap.of("set", element.getMetadataMap()));
        } else {
            updateNodeBuilder.put("metadata", ImmutableMap.of("set", ImmutableMap.<String, String>of()));
        }

        if (element.hasSource()) {
            updateNodeBuilder.put("source", ImmutableMap.of("set", element.getSource()));
        } else {
            updateNodeBuilder.put("source", ImmutableMap.of("setNull", true));
        }

        if (element.hasDataSetId()) {
            updateNodeBuilder.put("dataSetId", ImmutableMap.of("set", element.getDataSetId()));
        } else {
            updateNodeBuilder.put("dataSetId", ImmutableMap.of("setNull", true));
        }

        mapBuilder.put("update", updateNodeBuilder.build());
        return mapBuilder.build();
    }
}
