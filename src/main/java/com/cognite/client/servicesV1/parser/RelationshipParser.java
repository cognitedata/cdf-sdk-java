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

import com.cognite.client.dto.Relationship;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int64Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.cognite.client.servicesV1.ConnectorConstants.MAX_LOG_ELEMENT_LENGTH;

/**
 * This class contains a set of methods to help parsing file objects between Cognite api representations
 * (json and proto) and typed objects.
 */
public class RelationshipParser {
    private static final String logPrefix = "RelationshipParser - ";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final ImmutableBiMap<String, Relationship.ResourceType> resourceTypeMap = ImmutableBiMap
            .<String, Relationship.ResourceType>builder()
            .put("asset", Relationship.ResourceType.ASSET)
            .put("timeSeries", Relationship.ResourceType.TIME_SERIES)
            .put("file", Relationship.ResourceType.FILE)
            .put("event", Relationship.ResourceType.EVENT)
            .put("sequence", Relationship.ResourceType.SEQUENCE)
            .build();

    /**
     * Parses an relationship json string to {@link Relationship} proto object.
     *
     * @param json
     * @return
     * @throws Exception
     */
    public static Relationship parseRelationship(String json) throws Exception {
        String jsonExcerpt = json.substring(0, Math.min(json.length() - 1, MAX_LOG_ELEMENT_LENGTH));
        JsonNode root = objectMapper.readTree(json);
        Relationship.Builder relationshipBuilder = Relationship.newBuilder();

        // Required attributes.
        if (root.path("externalId").isTextual()) {
            relationshipBuilder.setExternalId(root.get("externalId").textValue());
        } else {
            throw new Exception(RelationshipParser.buildParsingExceptionString("externalId", jsonExcerpt));
        }
        if (root.path("sourceExternalId").isTextual()) {
            relationshipBuilder.setSourceExternalId(root.get("sourceExternalId").textValue());
        } else {
            throw new Exception(RelationshipParser.buildParsingExceptionString("sourceExternalId", jsonExcerpt));
        }
        if (root.path("sourceType").isTextual() && resourceTypeMap.containsKey(root.get("sourceType").textValue())) {
            relationshipBuilder.setSourceType(resourceTypeMap.get(root.get("sourceType").textValue()));
        } else {
            throw new Exception(RelationshipParser.buildParsingExceptionString("sourceType", jsonExcerpt));
        }
        if (root.path("targetExternalId").isTextual()) {
            relationshipBuilder.setTargetExternalId(root.get("targetExternalId").textValue());
        } else {
            throw new Exception(RelationshipParser.buildParsingExceptionString("targetExternalId", jsonExcerpt));
        }
        if (root.path("targetType").isTextual() && resourceTypeMap.containsKey(root.get("targetType").textValue())) {
            relationshipBuilder.setSourceType(resourceTypeMap.get(root.get("targetType").textValue()));
        } else {
            throw new Exception(RelationshipParser.buildParsingExceptionString("targetType", jsonExcerpt));
        }

        // The rest of the attributes are optional.
        if (root.path("startTime").isIntegralNumber()) {
            relationshipBuilder.setStartTime(Int64Value.of(root.get("startTime").longValue()));
        }
        if (root.path("endTime").isIntegralNumber()) {
            relationshipBuilder.setEndTime(Int64Value.of(root.get("endTime").longValue()));
        }
        if (root.path("confidence").isFloatingPointNumber()) {
            relationshipBuilder.setConfidence(FloatValue.of(root.get("confidence").floatValue()));
        }
        if (root.path("dataSetId").isIntegralNumber()) {
            relationshipBuilder.setDataSetId(Int64Value.of(root.get("dataSetId").longValue()));
        }
        if (root.path("createdTime").isIntegralNumber()) {
            relationshipBuilder.setCreatedTime(Int64Value.of(root.get("createdTime").longValue()));
        }
        if (root.path("lastUpdatedTime").isIntegralNumber()) {
            relationshipBuilder.setLastUpdatedTime(Int64Value.of(root.get("lastUpdatedTime").longValue()));
        }

        return relationshipBuilder.build();
    }

    /**
     * Builds a request insert item object from {@link Relationship}.
     *
     * An insert item object creates a new asset data object in the Cognite system.
     *
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestInsertItem(Relationship element) {
        Preconditions.checkNotNull(element, "Input cannot be null.");

        // Add all the mandatory attributes
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.<String, Object>builder()
                .put("externalId", element.getExternalId())
                .put("sourceExternalId", element.getSourceExternalId())
                .put("sourceType", resourceTypeMap.inverse().get(element.getSourceType()))
                .put("targetExternalId", element.getTargetExternalId())
                .put("targetType", resourceTypeMap.inverse().get(element.getTargetType()));

        // Add optional attributes
        if (element.hasStartTime()) {
            mapBuilder.put("startTime", element.getStartTime().getValue());
        }
        if (element.hasEndTime()) {
            mapBuilder.put("endTime", element.getEndTime().getValue());
        }
        if (element.hasConfidence()) {
            mapBuilder.put("confidence", element.getConfidence().getValue());
        }
        if (element.hasDataSetId()) {
            mapBuilder.put("dataSetId", element.getDataSetId().getValue());
        }
        if (element.hasDataSetId()) {
            mapBuilder.put("dataSetId", element.getDataSetId().getValue());
        }
        if (element.getLabelsCount() > 0) {
            List<Map<String, String>> labels = new ArrayList<>();
            for (String label : element.getLabelsList()) {
                labels.add(ImmutableMap.of("externalId", label));
            }
            mapBuilder.put("labels", labels);
        }

        return mapBuilder.build();
    }

    /**
     * Builds a request update item object from {@link Relationship}.
     *
     * An update item object updates an existing relationship object with new values for all provided fields.
     * Fields that are not in the update object retain their original value.
     *
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestUpdateItem(Relationship element) {
        Preconditions.checkNotNull(element, "Input cannot be null.");

        // Add all the mandatory attributes
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.<String, Object>builder()
                .put("externalId", element.getExternalId())
                .put("sourceExternalId", element.getSourceExternalId())
                .put("sourceType", resourceTypeMap.inverse().get(element.getSourceType()))
                .put("targetExternalId", element.getTargetExternalId())
                .put("targetType", resourceTypeMap.inverse().get(element.getTargetType()));

        // Add optional attributes
        if (element.hasStartTime()) {
            mapBuilder.put("startTime", element.getStartTime().getValue());
        }
        if (element.hasEndTime()) {
            mapBuilder.put("endTime", element.getEndTime().getValue());
        }
        if (element.hasConfidence()) {
            mapBuilder.put("confidence", element.getConfidence().getValue());
        }
        if (element.hasDataSetId()) {
            mapBuilder.put("dataSetId", element.getDataSetId().getValue());
        }
        if (element.hasDataSetId()) {
            mapBuilder.put("dataSetId", element.getDataSetId().getValue());
        }
        if (element.getLabelsCount() > 0) {
            List<Map<String, String>> labels = new ArrayList<>();
            for (String label : element.getLabelsList()) {
                labels.add(ImmutableMap.of("externalId", label));
            }
            mapBuilder.put("labels", labels);
        }

        return mapBuilder.build();
    }

    /**
     * Returns the string representation of a relationship reference resource type.
     * @param resourceType
     * @return
     */
    public static String toString(Relationship.ResourceType resourceType) {
        return resourceTypeMap.inverse().get(resourceType);
    }

    /**
     * Tries to parse a string into a {@code ResourceType}. If the string cannot be parsed, the returned
     * {@code Optional} will be empty.
     * @param type
     * @return
     */
    public static Optional<Relationship.ResourceType> parseResourceType(String type) {
        return Optional.ofNullable(resourceTypeMap.get(type));
    }

    private static String buildParsingExceptionString(String attribute, String json) {
        return logPrefix + "Unable to parse attribute [" + attribute + "]. Item excerpt: " + json;
    }
}
