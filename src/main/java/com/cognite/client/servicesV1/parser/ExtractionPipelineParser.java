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

import com.cognite.client.dto.ExtractionPipeline;
import com.cognite.client.dto.ExtractionPipelineRun;
import com.cognite.client.dto.Relationship;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;

import java.util.*;

import static com.cognite.client.servicesV1.ConnectorConstants.MAX_LOG_ELEMENT_LENGTH;

/**
 * This class contains a set of methods to help parse file objects between Cognite api representations
 * (json and proto) and typed objects.
 */
public class ExtractionPipelineParser {
    private static final String logPrefix = "ExtractionPipelineParser - ";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final ImmutableBiMap<String, ExtractionPipelineRun.Status> statusMap = ImmutableBiMap
            .<String, ExtractionPipelineRun.Status>builder()
            .put("success", ExtractionPipelineRun.Status.SUCCESS)
            .put("failure", ExtractionPipelineRun.Status.FAILURE)
            .put("seen", ExtractionPipelineRun.Status.SEEN)
            .build();

    /**
     * Parses a extraction pipeline json string to {@link ExtractionPipeline} proto object.
     *
     * @param json
     * @return
     * @throws Exception
     */
    public static ExtractionPipeline parseExtractionPipeline(String json) throws Exception {
        String jsonExcerpt = json.substring(0, Math.min(json.length() - 1, MAX_LOG_ELEMENT_LENGTH));
        JsonNode root = objectMapper.readTree(json);
        ExtractionPipeline.Builder pipelineBuilder = ExtractionPipeline.newBuilder();

        // Required attributes.
        if (root.path("externalId").isTextual()) {
            pipelineBuilder.setExternalId(root.get("externalId").textValue());
        } else {
            throw new Exception(ExtractionPipelineParser.buildParsingExceptionString("externalId", jsonExcerpt));
        }
        if (root.path("name").isTextual()) {
            pipelineBuilder.setName(root.get("name").textValue());
        } else {
            throw new Exception(ExtractionPipelineParser.buildParsingExceptionString("name", jsonExcerpt));
        }
        if (root.path("dataSetId").isIntegralNumber()) {
            pipelineBuilder.setDataSetId(root.get("dataSetId").longValue());
        } else {
            throw new Exception(ExtractionPipelineParser.buildParsingExceptionString("dataSetId", jsonExcerpt));
        }

        // The rest of the attributes are optional.
        if (root.path("id").isIntegralNumber()) {
            pipelineBuilder.setId(root.get("id").longValue());
        }
        if (root.path("description").isTextual()) {
            pipelineBuilder.setDescription(root.get("description").textValue());
        }

        if (root.path("schedule").isTextual()) {
            pipelineBuilder.setSchedule(root.get("schedule").textValue());
        }
        if (root.path("metadata").isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fieldIterator = root.path("metadata").fields();
            while (fieldIterator.hasNext()) {
                Map.Entry<String, JsonNode> entry = fieldIterator.next();
                if (entry.getValue().isTextual()) {
                    pipelineBuilder.putMetadata(entry.getKey(), entry.getValue().textValue());
                }
            }
        }
        if (root.path("source").isTextual()) {
            pipelineBuilder.setSource(root.get("source").textValue());
        }
        if (root.path("documentation").isTextual()) {
            pipelineBuilder.setDocumentation(root.get("documentation").textValue());
        }

        if (root.path("endTime").isIntegralNumber()) {
            relationshipBuilder.setEndTime(root.get("endTime").longValue());
        }
        if (root.path("confidence").isFloatingPointNumber()) {
            relationshipBuilder.setConfidence(root.get("confidence").floatValue());
        }
        if (root.path("dataSetId").isIntegralNumber()) {
            relationshipBuilder.setDataSetId(root.get("dataSetId").longValue());
        }
        if (root.path("createdTime").isIntegralNumber()) {
            relationshipBuilder.setCreatedTime(root.get("createdTime").longValue());
        }
        if (root.path("lastUpdatedTime").isIntegralNumber()) {
            relationshipBuilder.setLastUpdatedTime(root.get("lastUpdatedTime").longValue());
        }

        return relationshipBuilder.build();
    }

    if (root.path("targetType").isTextual() && resourceTypeMap.containsKey(root.get("targetType").textValue())) {
        relationshipBuilder.setSourceType(resourceTypeMap.get(root.get("targetType").textValue()));
    } else {
        throw new Exception(ExtractionPipelineParser.buildParsingExceptionString("targetType", jsonExcerpt));
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
        Preconditions.checkArgument(element.hasSourceExternalId(),
                "The relationship object must specify a source external id.");
        Preconditions.checkArgument(element.hasTargetExternalId(),
                "The relationship object must specify a target external id.");

        // Add all the mandatory attributes
        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.<String, Object>builder()
                .put("externalId", element.getExternalId())
                .put("sourceExternalId", element.getSourceExternalId())
                .put("sourceType", resourceTypeMap.inverse().get(element.getSourceType()))
                .put("targetExternalId", element.getTargetExternalId())
                .put("targetType", resourceTypeMap.inverse().get(element.getTargetType()));

        // Add optional attributes
        if (element.hasStartTime()) {
            mapBuilder.put("startTime", element.getStartTime());
        }
        if (element.hasEndTime()) {
            mapBuilder.put("endTime", element.getEndTime());
        }
        if (element.hasConfidence()) {
            mapBuilder.put("confidence", element.getConfidence());
        }
        if (element.hasDataSetId()) {
            mapBuilder.put("dataSetId", element.getDataSetId());
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

        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, Object> updateNodeBuilder = ImmutableMap.builder();

        // Add id reference
        mapBuilder.put("externalId", element.getExternalId());

        // Add the update fields
        if (element.hasSourceExternalId()) {
            updateNodeBuilder
                    .put("sourceExternalId", ImmutableMap.of("set", element.getSourceExternalId()))
                    .put("sourceType", ImmutableMap.of("set", resourceTypeMap.inverse().get(element.getSourceType())));
        }
        if (element.hasTargetExternalId()) {
            updateNodeBuilder
                    .put("targetExternalId", ImmutableMap.of("set", element.getTargetExternalId()))
                    .put("targetType", ImmutableMap.of("set", resourceTypeMap.inverse().get(element.getTargetType())));
        }
        if (element.hasStartTime()) {
            updateNodeBuilder.put("startTime", ImmutableMap.of("set", element.getStartTime()));
        }
        if (element.hasEndTime()) {
            updateNodeBuilder.put("endTime", ImmutableMap.of("set", element.getEndTime()));
        }
        if (element.hasConfidence()) {
            updateNodeBuilder.put("confidence", ImmutableMap.of("set", element.getConfidence()));
        }
        if (element.hasDataSetId()) {
            updateNodeBuilder.put("dataSetId", ImmutableMap.of("set", element.getDataSetId()));
        }

        if (element.getLabelsCount() > 0) {
            List<Map<String, String>> labels = new ArrayList<>();
            for (String label : element.getLabelsList()) {
                labels.add(ImmutableMap.of("externalId", label));
            }
            updateNodeBuilder.put("labels", ImmutableMap.of("add", labels));
        }
        mapBuilder.put("update", updateNodeBuilder.build());
        return mapBuilder.build();
    }

    /**
     * Builds a request replace item object from {@link Relationship}.
     *
     * A replace item object replaces an existing event object with new values for all provided fields.
     * Fields that are not in the update object are set to null.
     *
     * @param element
     * @return
     */
    public static Map<String, Object> toRequestReplaceItem(Relationship element) {
        Preconditions.checkNotNull(element, "Input cannot be null.");
        Preconditions.checkArgument(element.hasSourceExternalId(),
                "The relationship object must specify a source external id.");
        Preconditions.checkArgument(element.hasTargetExternalId(),
                "The relationship object must specify a target external id.");

        ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, Object> updateNodeBuilder = ImmutableMap.builder();

        // Add id reference
        mapBuilder.put("externalId", element.getExternalId());

        // Add the mandatory update replace fields
        updateNodeBuilder
                .put("sourceExternalId", ImmutableMap.of("set", element.getSourceExternalId()))
                .put("sourceType", ImmutableMap.of("set", resourceTypeMap.inverse().get(element.getSourceType())))
                .put("targetExternalId", ImmutableMap.of("set", element.getTargetExternalId()))
                .put("targetType", ImmutableMap.of("set", resourceTypeMap.inverse().get(element.getTargetType())));

        // Add the optional update replace fields
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

        if (element.hasConfidence()) {
            updateNodeBuilder.put("confidence", ImmutableMap.of("set", element.getConfidence()));
        } else {
            updateNodeBuilder.put("confidence", ImmutableMap.of("setNull", true));
        }

        if (element.hasDataSetId()) {
            updateNodeBuilder.put("dataSetId", ImmutableMap.of("set", element.getDataSetId()));
        } else {
            updateNodeBuilder.put("dataSetId", ImmutableMap.of("setNull", true));
        }

        List<Map<String, String>> labels = new ArrayList<>();
        for (String label : element.getLabelsList()) {
            labels.add(ImmutableMap.of("externalId", label));
        }
        updateNodeBuilder.put("labels", ImmutableMap.of("set", labels));

        mapBuilder.put("update", updateNodeBuilder.build());
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
